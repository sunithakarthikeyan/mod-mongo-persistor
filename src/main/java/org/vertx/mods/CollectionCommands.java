package org.vertx.mods;

import org.vertx.java.core.json.JsonObject;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

public class CollectionCommands {
    public JsonObject execute(DBCollection dbc, JsonObject message) {

        String action = message.getString("action");
        if (action == null) {
            return null;
        }

        switch (action) {
        case "ensureIndex":
            dbc.ensureIndex(jsonToDBObject(message.getObject("document")));
            return getAsJson("");

        case "count":
            Long l = dbc.count(jsonToDBObject(message.getObject("document")));
            return getAsJson(l);

        default:
            return null;
        }

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
            CollectionCommands cc = new CollectionCommands();
            JsonObject message = new JsonObject();
            message.putString("db", "test_db");
            message.putString("collection", "testcoll");
            message.putString("action", "count");
            message.putObject("document", new JsonObject());

            JsonObject j = cc.execute(dbc, message);
            System.out.println(j.toString());

        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}