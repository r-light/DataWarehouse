package HbaseToMysql;

import java.io.Serializable;

public class UserAggre implements Serializable {
    private String ID;
    private int EDU;
    private String INTER;
    private int PUBLISH;
    private int VIEW;
    private int COMMENT;
    private String LAST_TIME;

    public UserAggre() {
    }

    public UserAggre(String ID, int EDU, String INTER){
        this.ID = ID;
        this.EDU = EDU;
        this.INTER = INTER;
    }

    public UserAggre(String ID){
        this.ID = ID;
    }

    public int getPUBLISH() {
        return PUBLISH;
    }

    public void setPUBLISH(int PUBLISH) {
        this.PUBLISH = PUBLISH;
    }

    public int getVIEW() {
        return VIEW;
    }

    public void setVIEW(int VIEW) {
        this.VIEW = VIEW;
    }

    public int getCOMMENT() {
        return COMMENT;
    }

    public void setCOMMENT(int COMMENT) {
        this.COMMENT = COMMENT;
    }

    public String getLAST_TIME() {
        return LAST_TIME;
    }

    public void setLAST_TIME(String LAST_TIME) {
        this.LAST_TIME = LAST_TIME;
    }

    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public int getEDU() {
        return EDU;
    }

    public void setEDU(int EDU) {
        this.EDU = EDU;
    }

    public String getINTER() {
        return INTER;
    }

    public void setINTER(String INTER) {
        this.INTER = INTER;
    }
}
