package com.uwingstech;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jnrsmcu.sdk.netdevice.*;
import org.apache.commons.cli.*;
import org.fusesource.mqtt.client.*;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

enum Direction {
    North("North", "N", 0),
    NorthEast("NorthEast", "NE", 1),
    East("East", "E", 2),
    SouthEast("SouthEast", "SE", 3),
    South("South", "S", 4),
    SouthWest("SouthWest", "SE", 5),
    West("West", "W", 6),
    NorthWest("NorthWest", "NW", 7);

    private final String name;
    private final String shortName;
    private final int code;
    Direction(String name, String shortName, int code) {
        this.name = name;
        this.shortName = shortName;
        this.code = code;
    }

    public String getShortName() {
        return shortName;
    }

    public String getShortName(Direction obj) {
        for (Direction dir : values()) {
            if (dir.getCode() == obj.getCode()) {
                return dir.shortName;
            }
        }
        return null;
    }

    public int getCode() {
        return code;
    }

    public String toString() {
        return shortName;
    }

    public static Direction getDirection(int code) {
        for (Direction dir : values()) {
            if (dir.getCode() == code) {
                return dir;
            }
        }
        return null;
    }
}

// for debugging original from com.jnrsmcu.sdk.netdevice.BinarySerializeOpt
//class BinarySerializeOpt<T> {
//    BinarySerializeOpt() {
//    }
//    protected T deserialize(String fileName) {
//        try {
//            FileInputStream fileIn = new FileInputStream(fileName);
//            ObjectInputStream in = new ObjectInputStream(fileIn);
//            T res = (T)in.readObject();
//            in.close();
//            fileIn.close();
//            return res;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//    protected boolean serialize(T cls, String fileName) {
//        try {
//            FileOutputStream fileOut = new FileOutputStream(fileName);
//            ObjectOutputStream out = new ObjectOutputStream(fileOut);
//            out.writeObject(cls);
//            out.close();
//            fileOut.close();
//            return true;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return false;
//        }
//    }
//}

class WeatherStation {
    // 8 direction : { North, NorthEast, East, SouthEast,
    //                 South, SouthWest, West, NorthWest }
    public Direction  WindDirection;
    public double WindSpeed;
    public double Pripitation;
    public double Temperature;
    public double Humidity;
    public double Lux;
    public Date TimeStamp;
}

public class Main extends TimerTask {
    static final URL mainUrl = Main.class.getProtectionDomain().getCodeSource().getLocation();
    static WeatherStation ws = new WeatherStation();
    static boolean bMQTTConnection = false;
    static Timer timer = new Timer();
    static MQTT mqtt = new MQTT();
    static CallbackConnection connection;
    static ObjectMapper om = new ObjectMapper();
    static String mqttTopic;

    public static void main(String[] args) throws IOException,
            InterruptedException,
            MQTTException, URISyntaxException {

        Options options = new Options();
        options.addRequiredOption( "b", "broker", true, "MQTT Broker");
        options.addRequiredOption("t", "topic", true, "MQTT Message Topic");

        options.addOption("p", true, "port of MQTT Broker");
        options.addOption( "w", true, "Weather Station");
        String strBrokerHost = "";
        String strPortBroker = "1883";

        // load default param.dat from launch place
        String filePath = URLDecoder.decode(mainUrl.getPath(), "utf-8");
        System.out.println("Program launch from: " + filePath);
        if (filePath.endsWith(".jar")) {
            filePath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
        }
        filePath += "param.dat";
        //Map<Integer, ParamItem> paramCache =  new HashMap();
        //paramCache = (Map)(new BinarySerializeOpt()).deserialize(filePath);

        ws.Temperature = 25.0;
        ws.Lux = 100;
        //ws.WindDirection = Direction.NorthEast;
        ws.WindDirection = Direction.getDirection(2);


        CommandLineParser  cmdParser = new DefaultParser();
        try {
            CommandLine cmd = cmdParser.parse(options, args);
            strBrokerHost = cmd.getOptionValue("b");
            strPortBroker = cmd.getOptionValue("p", "1883");
            mqttTopic = cmd.getOptionValue("t");
        } catch (ParseException e) {
            System.out.println("Failed input arguments:\n\t" + e.getMessage());
            return;
        }

        System.out.println("Initial RSServer(Weather Station)");
        // initial server and load parameters from Binary Serial file
        RSServer rsServer = RSServer.Initiate(2404, filePath);// initial


        /*
        for (String arg : args) {
         System.out.println("args:" + arg);
        }
        */


        rsServer.addDataListener(new IDataListener() {// 添加监听
            @Override
            public void receiveTimmingAck(TimmingAck data) {// 校时指令应答处理
                System.out.println("Timesync->DeviceID:" + data.getDeviceId()
                        + "\tStatus：" + data.getStatus());
            }

            @Override
            public void receiveTelecontrolAck(TelecontrolAck data) {// 遥控指令应答处理
                System.out.println("TeleControl->DeviceID:" + data.getDeviceId()
                        + "\tRelayId:" + data.getRelayId() + "\tStatus:"
                        + data.getStatus());
            }

            @Override
            public void receiveStoreData(StoreData data) {// 已存储数据接收处理
                // 遍历节点数据。数据包括网络设备的数据以及各个节点数据。温湿度数据存放在节点数据中
                for (NodeData nd : data.getNodeList()) {
                    SimpleDateFormat sdf = new SimpleDateFormat(
                            "yy-MM-dd HH:mm:ss");
                    String str = sdf.format(nd.getRecordTime());
                    System.out.println("StoreData->DeviceId:" + data.getDeviceId()
                            + "\tNodeId:" + nd.getNodeId() + "\tTemperature:" + nd.getTem()
                            + "\tHumidity:" + nd.getHum() + "\tTime:" + str);
                }

            }

            @Override
            public void receiveRealtimeData(RealTimeData data) {// 实时数据接收处理
                // 遍历节点数据。数据包括网络设备的数据以及各个节点数据。温湿度数据存放在节点数据中
                for (NodeData nd : data.getNodeList()) {
                    System.out.println("RealTime->DeviceId:" + data.getDeviceId()
                            + "\tNodeId:" + nd.getNodeId() + "\tTemperature:" + nd.getTem()
                            + "\tHumidity:" + nd.getHum() + "\t经度:" + data.getLng()
                            + "\t纬度:" + data.getLat() + "\tCoordinateType:"
                            + data.getCoordinateType() + "\t继电器状态:"
                            + data.getRelayStatus());

                }

            }

            @Override
            public void receiveLoginData(LoginData data) {// 登录数据接收处理
                System.out.println("登录->设备地址:" + data.getDeviceId());

            }

            @Override
            public void receiveParamIds(ParamIdsData data) {
                String str = "设备参数编号列表->设备编号：" + data.getDeviceId()
                        + "\t参数总数量：" + data.getTotalCount() + "\t本帧参数数量："
                        + data.getCount() + "\r\n";
                for (int paramId : data.getPararmIdList())// 遍历设备中参数id编号
                {
                    str += paramId + ",";
                }
                System.out.println(str);

            }

            @Override
            public void receiveParam(ParamData data) {
                String str = "设备参数->设备编号：" + data.getDeviceId() + "\r\n";

                for (ParamItem pararm : data.getParameterList()) {
                    str += "参数编号："
                            + pararm.getParamId()
                            + "\t参数描述："
                            + pararm.getDescription()
                            + "\t参数值："
                            + (pararm.getValueDescription() == null ? pararm
                            .getValue() : pararm.getValueDescription()
                            .get(pararm.getValue())) + "\r\n";
                }
                System.out.println(str);

            }

            @Override
            public void receiveWriteParamAck(WriteParamAck data) {
                String str = "下载设备参数->设备编号：" + data.getDeviceId() + "\t参数数量："
                        + data.getCount() + "\t"
                        + (data.isSuccess() ? "下载成功" : "下载失败");
                System.out.println(str);

            }

            @Override
            public void receiveTransDataAck(TransDataAck data) {
                String str = "数据透传->设备编号：" + data.getDeviceId() + "\t响应结果："
                        + data.getData() + "\r\n字节数：" + data.getTransDataLen();
                System.out.println(str);

            }
        });

        // final CallbackConnection connection = mqtt.callbackConnection();
        /*
        connection.listener(new ExtendedListener() {
            public void onDisconnected() {
            }
            public void onConnected() {
                System.out.println("Connected MQTT Broker success!");
            }

            public void onPublish(UTF8Buffer topic, Buffer buffer, Runnable runnable) {
                //this.onFailure(createListenerNotSetError());
                // You can now process a received message from a topic.
                // Once process execute the ack runnable.
                runnable.run();
            }
            public void onPublish(UTF8Buffer topic, Buffer body, Callback<Callback<Void>> ack) {
                //this.onFailure(createListenerNotSetError());
                ack.notifyAll();
            }

            public void onFailure(Throwable value) {
                System.out.println("connection MQTT Broker failed!");
                Thread.currentThread().getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), value);
                //connection.close();

            }
        });
        */
        int port = Integer.parseInt(strPortBroker);
        System.out.println("Initial Mqtt client");
        System.out.println("connection to " + strBrokerHost + ", port:" + port);
        mqtt.setHost(strBrokerHost, port);
        mqtt.setKeepAlive((short)10);
        connection = mqtt.callbackConnection();
/*
        connection.listener(new Listener() {
            @Override
            public void onConnected() {
                System.out.println("Connected/Listener MQTT Broker success!");
            }

            @Override
            public void onDisconnected() {

            }

            @Override
            public void onPublish(UTF8Buffer utf8Buffer, Buffer buffer, Runnable runnable) {

            }

            @Override
            public void onFailure(Throwable throwable) {
                System.out.println("Connection MQTT Broker failed");
            }
        });
 */

        connection.connect(new Callback<Void>() {
            public void onFailure(Throwable value) {
                //result.failure(value); // If we could not connect to the server.
                System.out.println("Connection MQTT Broker failed");
            }

            // Once we connect..
            public void onSuccess(Void v) {
                System.out.println("Connected/Callback MQTT Broker success!");

                bMQTTConnection = true;
                timer.schedule(new Main(), 500, (60 * 1000)); // run publish message, 60sec

                // Subscribe to a topic
                /*
                Topic[] topics = {new Topic("foo", QoS.AT_LEAST_ONCE)};
                connection.subscribe(topics, new Callback<byte[]>() {
                    public void onSuccess(byte[] qoses) {
                        // The result of the subcribe request.
                    }
                    public void onFailure(Throwable value) {
                        //connection.close(null); // subscribe failed.
                    }
                });
                */

                // Send a message to a topic
                /*
                connection.publish("foo", "Hello".getBytes(), QoS.AT_LEAST_ONCE, false, new Callback<Void>() {
                    public void onSuccess(Void v) {
                        // the publish operation completed successfully.
                        System.out.println("publish on Success!");
                    }

                    public void onFailure(Throwable value) {
                        // connection.close(null); // publish failed.
                        System.out.println("publish  MQTT topic failed!");
                    }
                });
                */

                /*
                // To disconnect..
                connection.disconnect(new Callback<Void>() {
                    public void onSuccess(Void v) {
                        // called once the connection is disconnected.
                    }
                    public void onFailure(Throwable value) {
                        // Disconnects never fail.
                    }
                });
                */

            }
        });

        System.out.println("Connection RSServer(Weather Station)");
        rsServer.start();

    }

    @Override
    public void run() {
        try {
            ws.TimeStamp = new Date();
            String payload = om.writeValueAsString(ws);
            connection.publish(mqttTopic, payload.getBytes(), QoS.AT_LEAST_ONCE, false,
                    new Callback<Void>() {
                public void onSuccess(Void v) {
                    // the publish operation completed successfully.
                    System.out.println("publish to " + mqttTopic + " : " + payload );

                }
                public void onFailure(Throwable value) {
                    // connection.close(null); // publish failed.
                    System.out.println("publish  MQTT topic failed!");
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
