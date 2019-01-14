package net.bitnine.agensbrowser.web.message;

import net.bitnine.agensbrowser.web.persistence.outer.model.type.LabelType;
import org.json.simple.JSONObject;

import java.io.Serializable;

public class LabelDto extends ResponseDto implements Serializable {

    private static final long serialVersionUID = -6913045165388361075L;

    private RequestDto request;
    private LabelType label;

    public LabelDto() {
        super();
        this.request = null;
        this.label = new LabelType();
    }
    public LabelDto(RequestDto request) {
        super();
        this.request = request;
        this.label = new LabelType();
    }
    public LabelDto(RequestDto request, LabelType label) {
        super();
        this.request = request;
        this.label = label;
    }

    public RequestDto getRequest() { return request; }
    public LabelType getLabel() {
        return label;
    }

    public void setRequest(RequestDto request) { this.request = request; }
    public void setLabel(LabelType label){ this.label = label; }

    @Override
    public String toString(){ return "{\"label\": "+ toJson().toJSONString() + "}"; }

    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("group", "label_info");
        json.put("state", state.toString());
        json.put("message", message);
        if( request != null ) json.put("request", request.toJson());

        if( label != null ) json.put("label", label.toJson());
        else json.put("label", (new LabelType()).toJson());

        return json;
    }

}