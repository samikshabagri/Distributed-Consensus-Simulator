#ifndef WEBSERVER_H
#define WEBSERVER_H

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <fstream>
#include <sstream>
#include <string>
#include <iomanip>
#include "Cluster.h"
#include "LoadBalancer.h"

using namespace std;

class WebServer {
private:
    Cluster& cluster;
    LoadBalancer& lb;
    int port;

    static string escapeJson(const string& s) {
        string out;
        for (char c : s) {
            if (c == '"') out += "\\\"";
            else if (c == '\\') out += "\\\\";
            else if (c == '\n' || c == '\r') out += ' ';
            else out += c;
        }
        return out;
    }

    static string getParam(const string& query, const string& key) {
        string token = key + "=";
        size_t pos = query.find(token);
        if (pos == string::npos) return "";
        pos += token.size();
        size_t end = query.find('&', pos);
        string val = (end == string::npos) ? query.substr(pos) : query.substr(pos, end - pos);
        for (char& c : val) if (c == '+') c = ' ';
        return val;
    }

    static string readFrontendFile(const string& name) {
        ifstream f("frontend/" + name);
        if (!f) f.open("../frontend/" + name);
        if (!f) return "";
        ostringstream ss;
        ss << f.rdbuf();
        return ss.str();
    }

    static void sendHttp(SOCKET client, const string& contentType, const string& body) {
        ostringstream res;
        res << "HTTP/1.1 200 OK\r\n"
            << "Content-Type: " << contentType << "\r\n"
            << "Access-Control-Allow-Origin: *\r\n"
            << "Content-Length: " << body.size() << "\r\n"
            << "Connection: close\r\n\r\n"
            << body;
        string raw = res.str();
        ::send(client, raw.c_str(), (int)raw.size(), 0);
    }

    string buildStatusJson() {
        RaftNode* leader = cluster.findLeader();
        int leaderId = leader ? leader->getId() : -1;
        int leaderTerm = leader ? leader->getTerm() : 0;
        vector<NodeStatus> statuses = cluster.getStatuses();
        LoadSnapshot ls = lb.getSnapshot();
        vector<string> logs = Logger::get().getRecent();

        // Pick balances from leader or first live node
        map<string, double> balances;
        for (const auto& st : statuses) {
            if (!st.crashed) {
                balances = st.ledger.getAll();
                if (st.id == leaderId) break;
            }
        }

        ostringstream j;
        j << fixed << setprecision(2);
        j << "{";
        j << "\"leaderId\":" << leaderId << ",";
        j << "\"term\":" << leaderTerm << ",";

        j << "\"lb\":{";
        j << "\"currentLoad\":" << ls.currentLoad << ",";
        j << "\"threshold\":" << ls.threshold << ",";
        j << "\"totalReads\":" << ls.totalReads << ",";
        j << "\"totalWrites\":" << ls.totalWrites << ",";
        j << "\"bursting\":" << (ls.bursting ? "true" : "false") << ",";
        j << "\"burstSent\":" << ls.burstSent << ",";
        j << "\"burstTotal\":" << ls.burstTotal << ",";
        j << "\"lastReadAcc\":\"" << escapeJson(ls.lastReadAcc) << "\",";
        j << "\"lastReadNode\":" << ls.lastReadNode << ",";
        j << "\"lastReadBal\":" << ls.lastReadBal << ",";
        j << "\"history\":[";
        for (size_t i = 0; i < ls.loadHistory.size(); ++i) {
            if (i) j << ",";
            j << ls.loadHistory[i];
        }
        j << "]},";

        j << "\"nodes\":[";
        for (size_t i = 0; i < statuses.size(); ++i) {
            const auto& n = statuses[i];
            if (i) j << ",";
            j << "{"
              << "\"id\":" << n.id << ","
              << "\"state\":\"" << (n.crashed ? "CRASHED" : stateName(n.state)) << "\","
              << "\"term\":" << n.term << ","
              << "\"logLen\":" << n.lastLogIndex << ","
              << "\"commitIndex\":" << n.commitIndex << ","
              << "\"lastApplied\":" << n.lastApplied << ","
              << "\"crashed\":" << (n.crashed ? "true" : "false") << ","
              << "\"autoScaled\":" << (n.autoScaled ? "true" : "false") << ","
              << "\"readsHandled\":" << n.readsHandled << ","
              << "\"writesHandled\":" << n.writesHandled
              << "}";
        }
        j << "],";

        j << "\"balances\":{";
        bool firstBal = true;
        for (const auto& kv : balances) {
            if (!firstBal) j << ",";
            firstBal = false;
            j << "\"" << escapeJson(kv.first) << "\":" << kv.second;
        }
        j << "},";

        j << "\"logs\":[";
        for (size_t i = 0; i < logs.size(); ++i) {
            if (i) j << ",";
            j << "\"" << escapeJson(logs[i]) << "\"";
        }
        j << "]";
        j << "}";
        return j.str();
    }

    void handleRequest(SOCKET client) {
        char buf[2048];
        int n = ::recv(client, buf, sizeof(buf) - 1, 0);
        if (n <= 0) { ::closesocket(client); return; }
        buf[n] = '\0';

        string req(buf);
        size_t sp1 = req.find(' ');
        size_t sp2 = (sp1 == string::npos) ? string::npos : req.find(' ', sp1 + 1);
        string target = (sp1 != string::npos && sp2 != string::npos)
                        ? req.substr(sp1 + 1, sp2 - sp1 - 1) : "/";

        string path = target, query;
        size_t qpos = target.find('?');
        if (qpos != string::npos) {
            path = target.substr(0, qpos);
            query = target.substr(qpos + 1);
        }

        if (path == "/" || path == "/index.html") {
            string html = readFrontendFile("index.html");
            sendHttp(client, "text/html; charset=utf-8", html.empty() ? "<h1>frontend/index.html not found</h1>" : html);
        } else if (path == "/styles.css" || path == "/tokens.css" || path == "/base.css") {
            sendHttp(client, "text/css; charset=utf-8", readFrontendFile(path.substr(1)));
        } else if (path == "/status") {
            sendHttp(client, "application/json", buildStatusJson());
        } else if (path == "/write") {
            Transaction t;
            string type = getParam(query, "type");
            t.user = getParam(query, "user");
            if (t.user.empty()) t.user = "Client";
            t.account = getParam(query, "acc");
            t.toAccount = getParam(query, "to");
            string amtStr = getParam(query, "amt");
            t.amount = amtStr.empty() ? 0.0 : atof(amtStr.c_str());

            if (type == "WITHDRAW") t.type = TxnType::WITHDRAW;
            else if (type == "TRANSFER") t.type = TxnType::TRANSFER;
            else t.type = TxnType::DEPOSIT;

            int leaderId = -1;
            bool ok = lb.handleWrite(t, leaderId);
            ostringstream r;
            r << "{\"ok\":" << (ok ? "true" : "false") << ",\"leaderId\":" << leaderId << "}";
            sendHttp(client, "application/json", r.str());
        } else if (path == "/read") {
            string acc = getParam(query, "acc");
            int followerId = -1;
            double bal = 0.0;
            bool ok = lb.handleRead(acc, followerId, bal);
            ostringstream r;
            r << fixed << setprecision(2)
              << "{\"ok\":" << (ok ? "true" : "false")
              << ",\"followerId\":" << followerId
              << ",\"account\":\"" << escapeJson(acc) << "\""
              << ",\"balance\":" << bal << "}";
            sendHttp(client, "application/json", r.str());
        } else if (path == "/burst") {
            string nStr = getParam(query, "n");
            int count = nStr.empty() ? 3000 : atoi(nStr.c_str());
            lb.startBurst(count);
            sendHttp(client, "application/json", "{\"ok\":true}");
        } else if (path == "/crash") {
            int id = atoi(getParam(query, "id").c_str());
            cluster.crashNode(id);
            sendHttp(client, "application/json", "{\"ok\":true}");
        } else if (path == "/recover") {
            int id = atoi(getParam(query, "id").c_str());
            cluster.recoverNode(id);
            sendHttp(client, "application/json", "{\"ok\":true}");
        } else {
            sendHttp(client, "text/plain", "Not Found");
        }

        ::closesocket(client);
    }

public:
    WebServer(Cluster& c, LoadBalancer& balancer, int p = 8080)
        : cluster(c), lb(balancer), port(p) {}

    void start() {
        WSADATA wsa;
        WSAStartup(MAKEWORD(2, 2), &wsa);

        SOCKET serverSock = ::socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        ::setsockopt(serverSock, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons((u_short)port);

        if (::bind(serverSock, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
            Logger::get().log("Failed to bind port " + to_string(port));
            WSACleanup();
            return;
        }
        ::listen(serverSock, 16);

        Logger::get().log("Web dashboard ready at http://localhost:" + to_string(port));

        while (true) {
            SOCKET client = ::accept(serverSock, nullptr, nullptr);
            if (client != INVALID_SOCKET) handleRequest(client);
        }

        ::closesocket(serverSock);
        WSACleanup();
    }
};

#endif
