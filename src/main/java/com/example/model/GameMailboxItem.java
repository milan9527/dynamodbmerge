package com.example.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GameMailboxItem implements Serializable {
    private static final long serialVersionUID = 1L;

    private String mailId;
    private Integer addRewardType;
    private String claimAt;
    private Boolean claimed;
    private Map<String, Map<String, String>> placeholderData;
    private List<Reward> rewards;
    private String testUserId;
    private String userId;

    // Default constructor required for Jackson
    public GameMailboxItem() {}

    public String getMailId() {
        return mailId;
    }

    public void setMailId(String mailId) {
        this.mailId = mailId;
    }

    public Integer getAddRewardType() {
        return addRewardType;
    }

    public void setAddRewardType(Integer addRewardType) {
        this.addRewardType = addRewardType;
    }

    public String getClaimAt() {
        return claimAt;
    }

    public void setClaimAt(String claimAt) {
        this.claimAt = claimAt;
    }

    public Boolean getClaimed() {
        return claimed;
    }

    public void setClaimed(Boolean claimed) {
        this.claimed = claimed;
    }

    public Map<String, Map<String, String>> getPlaceholderData() {
        return placeholderData;
    }

    public void setPlaceholderData(Map<String, Map<String, String>> placeholderData) {
        this.placeholderData = placeholderData;
    }

    public List<Reward> getRewards() {
        return rewards;
    }

    public void setRewards(List<Reward> rewards) {
        this.rewards = rewards;
    }

    public String getTestUserId() {
        return testUserId;
    }

    public void setTestUserId(String testUserId) {
        this.testUserId = testUserId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Reward implements Serializable {
        private static final long serialVersionUID = 1L;

        private Integer amount;
        private String id;
        private Boolean isPaReward;
        private String rewardId;
        private Integer rewardType;

        // Default constructor required for Jackson
        public Reward() {}

        public Integer getAmount() {
            return amount;
        }

        public void setAmount(Integer amount) {
            this.amount = amount;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Boolean getIsPaReward() {
            return isPaReward;
        }

        public void setIsPaReward(Boolean isPaReward) {
            this.isPaReward = isPaReward;
        }

        public String getRewardId() {
            return rewardId;
        }

        public void setRewardId(String rewardId) {
            this.rewardId = rewardId;
        }

        public Integer getRewardType() {
            return rewardType;
        }

        public void setRewardType(Integer rewardType) {
            this.rewardType = rewardType;
        }
    }
}
