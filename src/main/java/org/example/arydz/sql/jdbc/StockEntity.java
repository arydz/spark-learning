package org.example.arydz.sql.jdbc;

import java.io.Serializable;

public class StockEntity implements Serializable {

    private Integer id;
    private String ticket;
    private Double avg_volume;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTicket() {
        return ticket;
    }

    public void setTicket(String ticket) {
        this.ticket = ticket;
    }

    public Double getAvg_volume() {
        return avg_volume;
    }

    public void setAvg_volume(Double avg_volume) {
        this.avg_volume = avg_volume;
    }
}
