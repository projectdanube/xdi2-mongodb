package xdi2.core.impl.json.mongodb;

import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;


/** 
 * Helper Class for creating MongoClientOptions
 *
 */
public class MongoClientOptionsFactory {
    
    private static final int PRIMARY_READ_PREFERENCE = 1;
    private static final int PRIMARY_PREFERRED_READ_PREFERENCE = 2;
    private static final int SECONDARY_READ_PREFERENCE = 3;
    private static final int SECONDARY_PREFERRED_READ_PREFERENCE = 4;
    private static final int NEAREST_READ_PREFERENCE = 5;
    
    private static final int UNACKNOWLEDGED_WRITE_CONCERN = 1;
    private static final int ACKNOWLEDGED_WRITE_CONCERN = 2;
    private static final int REPLICA_ACKNOWLEDGED_WRITE_CONCERN = 3;

    /** */
    private Integer connectionsPerHost;
    
    /** */
    private Integer connectTimeout;

    /** */
    private Boolean autoConnectRetry;

    /** */
    private Integer maxWaitTime;
    
    /** */
    private Integer socketTimeout;

    /** */
    private Integer maxAutoConnectRetryTime;

    /** */
    private Integer writeConcern;
    
    /** */
    private Integer readPreference;
    
    /**
     * @return the connectionsPerHost
     */
    public Integer getConnectionsPerHost() {
        return connectionsPerHost;
    }

    /**
     * @param connectionsPerHost the connectionsPerHost to set
     */
    public void setConnectionsPerHost(Integer connectionsPerHost) {
        this.connectionsPerHost = connectionsPerHost;
    }

    /**
     * @return the connectTimeout
     */
    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * @param connectTimeout the connectTimeout to set
     */
    public void setConnectTimeout(Integer connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * @return the autoConnectRetry
     */
    public Boolean isAutoConnectRetry() {
        return autoConnectRetry;
    }

    /**
     * @param autoConnectRetry the autoConnectRetry to set
     */
    public void setAutoConnectRetry(Boolean autoConnectRetry) {
        this.autoConnectRetry = autoConnectRetry;
    }

    /**
     * @return the maxWaitTime
     */
    public Integer getMaxWaitTime() {
        return maxWaitTime;
    }

    /**
     * @param maxWaitTime the maxWaitTime to set
     */
    public void setMaxWaitTime(Integer maxWaitTime) {
        this.maxWaitTime = maxWaitTime;
    }

    /**
     * @return the socketTimeout
     */
    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    /**
     * @param socketTimeout the socketTimeout to set
     */
    public void setSocketTimeout(Integer socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    /**
     * @return the maxAutoConnectRetryTime
     */
    public Integer getMaxAutoConnectRetryTime() {
        return maxAutoConnectRetryTime;
    }

    /**
     * @param maxAutoConnectRetryTime the maxAutoConnectRetryTime to set
     */
    public void setMaxAutoConnectRetryTime(Integer maxAutoConnectRetryTime) {
        this.maxAutoConnectRetryTime = maxAutoConnectRetryTime;
    }

    /**
     * @return the writeConcern
     */
    public Integer getWriteConcern() {
        return writeConcern;
    }

    /**
     * @param writeConcern the writeConcern to set
     */
    public void setWriteConcern(Integer writeConcern) {
        this.writeConcern = writeConcern;
    }

    /**
     * @return the readPreference
     */
    public Integer getReadPreference() {
        return readPreference;
    }

    /**
     * @param readPreference the readPreference to set
     */
    public void setReadPreference(Integer readPreference) {
        this.readPreference = readPreference;
    }

    /** Factory Method */
    public MongoClientOptions createMongoClientOptions() {
        
        MongoClientOptions.Builder clientOptionsBuilder = MongoClientOptions.builder();
        
        if (connectionsPerHost != null) {
            clientOptionsBuilder.connectionsPerHost(connectionsPerHost);
        }
        
        if (connectTimeout != null) {
            clientOptionsBuilder.connectTimeout(connectTimeout);
        }
        
        if (autoConnectRetry != null) {
            clientOptionsBuilder.autoConnectRetry(autoConnectRetry);
        }
        
        if (maxWaitTime != null) {
            clientOptionsBuilder.maxWaitTime(maxWaitTime);
        }
        
        if (socketTimeout != null) {
            clientOptionsBuilder.socketTimeout(socketTimeout);
        }
        
        if (maxAutoConnectRetryTime != null) {
            clientOptionsBuilder.maxAutoConnectRetryTime(maxAutoConnectRetryTime);
        }
        
        if (readPreference != null) {
            switch (readPreference) {         
                case PRIMARY_READ_PREFERENCE:
                    clientOptionsBuilder.readPreference(ReadPreference.primary());
                    break;                   
                case PRIMARY_PREFERRED_READ_PREFERENCE:
                    clientOptionsBuilder.readPreference(ReadPreference.primaryPreferred());
                    break;
                case NEAREST_READ_PREFERENCE:
                    clientOptionsBuilder.readPreference(ReadPreference.nearest());
                    break;
                case SECONDARY_READ_PREFERENCE:
                    clientOptionsBuilder.readPreference(ReadPreference.secondary());
                    break;
                case SECONDARY_PREFERRED_READ_PREFERENCE:
                    clientOptionsBuilder.readPreference(ReadPreference.secondaryPreferred());
                    break;
                default:
                    clientOptionsBuilder.readPreference(ReadPreference.primary());
                    break;
            }
        } 
            
        
        if (writeConcern != null) {
            switch (writeConcern) { 
            case UNACKNOWLEDGED_WRITE_CONCERN:
                clientOptionsBuilder.writeConcern(WriteConcern.UNACKNOWLEDGED);
                break;                   
            case ACKNOWLEDGED_WRITE_CONCERN:
                clientOptionsBuilder.writeConcern(WriteConcern.ACKNOWLEDGED);
                break;
            case REPLICA_ACKNOWLEDGED_WRITE_CONCERN:
                clientOptionsBuilder.writeConcern(WriteConcern.REPLICAS_SAFE);
                break;
            }
        }
        
        MongoClientOptions clientOptions = clientOptionsBuilder.build(); 
        
        return clientOptions;
    }
    

    
    
    

}
