package org.vertx.mods;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.vertx.java.core.json.JsonObject;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

public class DynaCollectionCommands {

    public JsonObject execute(DBCollection dbc, JsonObject message) {

        String command = message.getString("command");
        List<Object> params = new ArrayList<>();
        params.add(jsonToDBObject(message.getObject("document")));

        try {
            Class<?> c = dbc.getClass();
            Method method = c.getMethod(command, getParamClasses(params));
            return getAsJson(method.invoke(dbc, params.toArray()));

        } catch (IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException
                | SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    private Class<?>[] getParamClasses(List<Object> params) {
        ArrayList<Class<?>> c = new ArrayList<>();
        for (Object o : params) {
            if (o.getClass().getSimpleName().equals("BasicDBObject")) {
                c.add(DBObject.class);
            } else {
                c.add(o.getClass());
            }
        }
        return c.toArray(new Class[params.size()]);
    }

    private JsonObject getAsJson(Object o) {
        return new JsonObject().putString("result",
                o == null ? "" : o.toString());
    }

    private DBObject jsonToDBObject(JsonObject object) {
        String str = object.encode();
        return (DBObject) JSON.parse(str);
    }

    public static void main(String args[]) {
        try {
            Mongo mongo = new Mongo("127.0.0.1");
            DB db = mongo.getDB("test_db");
            DBCollection dbc = db.getCollection("testcoll");

            JsonObject message = new JsonObject();
            message.putString("db", "test_db");
            message.putString("collection", "testcoll");
            message.putString("action", "command");
            message.putString("command", "count");
            message.putObject("document", new JsonObject());

            DynaCollectionCommands cc = new DynaCollectionCommands();
            JsonObject j = cc.execute(dbc, message);
            System.out.println(j.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}