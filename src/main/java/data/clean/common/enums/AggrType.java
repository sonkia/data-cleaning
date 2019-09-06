package data.clean.common.enums;

public enum AggrType {
    MEAN(1),
    MEDIAN(2),
    SUM(3),
    FIRST(4),
    LATEST(5);

    private int code;

    AggrType(int i) {
    }

    public int getCode() {
        return code;
    }

}
