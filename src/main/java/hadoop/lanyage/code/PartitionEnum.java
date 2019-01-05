package hadoop.lanyage.code;

import java.util.HashMap;
import java.util.Map;

public enum PartitionEnum {
    MATH(0), ALGORITHM(1), COMPUTER(2), ENGLISH(3);
    private int code;
    private static Map<String, Integer> cache = new HashMap<>();

    public int getCode() {
        return code;
    }

    PartitionEnum(int code) {
        this.code = code;
    }

    static {
        cache.put("math", MATH.code);
        cache.put("algorithm", ALGORITHM.code);
        cache.put("computer", COMPUTER.code);
        cache.put("english", ENGLISH.code);
    }

    public static int get(String s) {
        if (!cache.containsKey(s)) {
            throw new RuntimeException("此Key不存在:" + s +". 请尝试{math, algorithm, computer, english}");
        }
        return cache.get(s);
    }
}
