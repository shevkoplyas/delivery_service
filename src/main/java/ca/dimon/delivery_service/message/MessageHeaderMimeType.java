package ca.dimon.delivery_service.message;

import com.google.gson.annotations.SerializedName;

public enum MessageHeaderMimeType {
    
    // Note: these "@SerializeName" annotations are part of gson and they'd make gson.toJson(this) to produce specified string instead of "PUBLISH" string.
    @SerializedName("message_header/publish")
    PUBLISH("message_header/publish"),
    
    @SerializedName("message_header/request")
    REQUEST("message_header/request"),
    
    @SerializedName("message_header/response")
    RESPONSE("message_header/response");
    
    private final String mime_type;       

    private MessageHeaderMimeType(String mime_type) {
        this.mime_type = mime_type;
    }

    public boolean equalsName(String other_mime_type) {
        // (otherName == null) check is not needed because name.equals(null) returns false 
        return mime_type.equals(other_mime_type);
    }

    public String toString() {
       return this.mime_type;
    }
}
