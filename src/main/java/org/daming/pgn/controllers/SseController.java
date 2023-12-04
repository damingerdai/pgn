package org.daming.pgn.controllers;

import org.daming.pgn.emitters.SseEmitters;
import org.postgresql.PGNotification;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.postgresql.PGConnection;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author gming001
 * @version 2023-12-04 13:33
 */
@RestController
@RequestMapping("/datainterface")
public class SseController {

    private final SseEmitters emitters = new SseEmitters();

    private List<Map<String,String>> events = new ArrayList<Map<String,String>>();

    private final ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(1);

    private Map<String, PGConnection> pgConns = new HashMap<String, PGConnection>();

    private Boolean initStatus = true;

    // 建立SSE连接，这里为了简化，将JDBC连接通过URL参数传入，这样做是不安全的
    @RequestMapping(value = "/sse/listen", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter listenToBroadcast() throws SQLException, ClassNotFoundException {
        if (this.initStatus) {
            this.initStatus = false;
            broadcast();
        }
        this.initDbListener("jdbc:postgresql://127.0.0.1:5432/postgres");
        return emitters.add();
    }

    public Boolean initDbListener(String url) throws SQLException, ClassNotFoundException {
        // 如果连接池已存在此连接，则跳过
        if (this.pgConns.get(url) != null) {
            return false;
        }
        Class.forName("org.postgresql.Driver");
        Connection conn = DriverManager.getConnection(url,"postgres","123456");
        org.postgresql.PGConnection pgConn = conn.unwrap(org.postgresql.PGConnection.class);
        // 将pg数据库连接存入连接池pgConn
        this.pgConns.put(url, pgConn);
        // 执行监听change_data_capture频道
        Statement stmt = conn.createStatement();
        stmt.execute("LISTEN change_data_capture");
        stmt.close();
        return true;
    }

    // 顺手实现了广播接口，从前端也可以发起全域广播
    @RequestMapping(value = "/sse/broadcast")
    public Boolean addToBroadcast(@RequestParam String type, @RequestParam String message) {
        if (message != null && message != "") {
            Map<String,String> typeMessage = new HashMap<String,String>();
            typeMessage.put("type", type);
            typeMessage.put("message", message);
            events.add(typeMessage);
            return true;
        } else {
            return false;
        }
    }

    // 初始化广播
    private void broadcast() throws SQLException {
        scheduledThreadPool.scheduleAtFixedRate(() -> {
            try {
                // 定时接收数据库通知
                this.pgConns.forEach( (dbid,pgConn) -> {
                    PGNotification notifications[] = null;
                    try {
                        notifications = pgConn.getNotifications();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    if (notifications != null) {
                        for (int i = 0; i < notifications.length; i++) {
                            String message = notifications[i].getParameter();
                            // 这里做了重复信息过滤
                            if (i > 0) {
                                String previousMessage = notifications[i-1].getParameter();
                                if (Objects.equals(message, previousMessage)) {
                                    continue;
                                }
                            }
                            this.addToBroadcast(dbid, notifications[i].getParameter());
                        }
                    }
                });
                // 如果队列中有消息，向所有客户端推送
                if (!events.isEmpty()) {
                    Map<String,String> firstEvent = events.remove(0);
                    String type = firstEvent.get("type");
                    String message = firstEvent.get("message");
                    emitters.send(
                            SseEmitter.event()
                                    .id(UUID.randomUUID().toString())
                                    .name(type)
                                    .data(message)
                    );
                }
            } catch (Exception e) {}
        }, 0, 1, TimeUnit.SECONDS);
    }
}
