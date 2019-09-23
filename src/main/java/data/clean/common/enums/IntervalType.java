package data.clean.common.enums;

public enum IntervalType {
    SECOND(1),
    MINUTE(2),
    HOUR(3),
    DAY(4);

    private int code;

    IntervalType(int i) {
    }

    public int getCode() {
        return code;
    }

}
