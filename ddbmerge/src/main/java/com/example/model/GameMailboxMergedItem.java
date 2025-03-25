package com.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GameMailboxMergedItem extends GameMailboxItem implements Serializable {
    private static final long serialVersionUID = 1L;

    private String PK;
    private String PK1;
    private String PK2;
    private String gameId;

    // Default constructor required for Jackson
    public GameMailboxMergedItem() {
        super();
    }

    // Constructor to convert from source item
    public GameMailboxMergedItem(GameMailboxItem source, String gameId) {
        // Copy all fields from the source item
        this.setMailId(source.getMailId());
        this.setAddRewardType(source.getAddRewardType());
        this.setClaimAt(source.getClaimAt());
        this.setClaimed(source.getClaimed());
        this.setPlaceholderData(source.getPlaceholderData());
        this.setRewards(source.getRewards());
        this.setTestUserId(source.getTestUserId());
        this.setUserId(source.getUserId());
        
        // Set new fields
        this.gameId = gameId;
        
        // Generate partition keys
        this.PK = gameId + "#" + source.getMailId();
        
        // Extract the part before UserId_ for PK1
        String mailId = source.getMailId();
        int userIdIndex = mailId.indexOf("UserId_");
        if (userIdIndex > 0) {
            // Find the part before UserId_
            String beforeUserId = mailId.substring(0, userIdIndex);
            // Remove the user ID prefix if it exists (e.g., "p:6225391_")
            if (beforeUserId.contains("_")) {
                beforeUserId = beforeUserId.substring(beforeUserId.indexOf("_") + 1);
            }
            this.PK1 = gameId + "#" + beforeUserId;
        } else {
            // Fallback if format is different
            this.PK1 = gameId + "#" + mailId;
        }
        
        // PK2 is gameId#userId
        this.PK2 = gameId + "#" + source.getUserId();
    }

    public String getPK() {
        return PK;
    }

    public void setPK(String PK) {
        this.PK = PK;
    }

    public String getPK1() {
        return PK1;
    }

    public void setPK1(String PK1) {
        this.PK1 = PK1;
    }

    public String getPK2() {
        return PK2;
    }

    public void setPK2(String PK2) {
        this.PK2 = PK2;
    }

    public String getGameId() {
        return gameId;
    }

    public void setGameId(String gameId) {
        this.gameId = gameId;
    }
}
