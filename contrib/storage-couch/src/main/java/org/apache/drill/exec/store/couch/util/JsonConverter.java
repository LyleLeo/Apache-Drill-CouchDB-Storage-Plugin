package org.apache.drill.exec.store.couch.util;

import java.io.FileInputStream;
import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;

public class JsonConverter {
    public static JsonNode parse(String content, String key) {
        String []path = key.split("/");
        try {
            JsonNode node = from(content);
            for (String p : path) {
                if (node == null) {
                    return null;
                }
                node = node.get(p);
            }
            return node;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static JsonNode parse(String content) {
        try {
            JsonNode root = from(content);
            if (root.isArray()) {
                return root;
            }
            return null;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @SuppressWarnings("resource")
    public static String stringFromFile(String file) {
        try {
            FileInputStream stream = new FileInputStream(file);
            int size = stream.available();
            byte[] bytes = new byte[size];
            stream.read(bytes);
            return new String(bytes, Charsets.UTF_8);
        } catch (IOException e) {
        }
        return "";
    }

    private static JsonNode from(String content) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(content);
        return root;
    }
}