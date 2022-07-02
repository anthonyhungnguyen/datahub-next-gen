package exception;

public class ErrorResponseException extends RuntimeException{
    private int code;

    public ErrorResponseException(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }
}
