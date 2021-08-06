package com.autocomplete;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBOutputWritable implements DBWritable {

    private String startingPhrase;
    private String followingPhrase;
    private int count;

    public DBOutputWritable() {
    }

    public String getStartingPhrase() {
        return startingPhrase;
    }

    public void setStartingPhrase(String startingPhrase) {
        this.startingPhrase = startingPhrase;
    }

    public String getFollowingPhrase() {
        return followingPhrase;
    }

    public void setFollowingPhrase(String followingPhrase) {
        this.followingPhrase = followingPhrase;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public DBOutputWritable(String startingPhrase, String followingPhrase, int count) {
        this.startingPhrase = startingPhrase;
        this.followingPhrase = followingPhrase;
        this.count = count;
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.startingPhrase);
        statement.setString(2, this.followingPhrase);
        statement.setInt(3, this.count);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.startingPhrase = resultSet.getString(1);
        this.followingPhrase = resultSet.getString(2);
        this.count = resultSet.getInt(3);
    }
}
