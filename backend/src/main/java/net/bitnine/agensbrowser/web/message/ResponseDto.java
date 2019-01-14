package net.bitnine.agensbrowser.web.message;

import org.json.simple.JSONObject;

import java.io.Serializable;

public class ResponseDto implements Serializable {

    private static final long serialVersionUID = 2903279623511697162L;

    public static enum StateType { PENDING, SUCCESS, FAIL, KILLED, NONE };
    protected StateType state;
    protected String message;
    protected String _link;

    public static final StateType toStateType(String state){
        if( state.equalsIgnoreCase(StateType.PENDING.toString()) ) return StateType.PENDING;
        else if( state.equalsIgnoreCase(StateType.SUCCESS.toString()) ) return StateType.SUCCESS;
        else if( state.equalsIgnoreCase(StateType.FAIL.toString()) ) return StateType.FAIL;
        else if( state.equalsIgnoreCase(StateType.KILLED.toString()) ) return StateType.KILLED;
        else return StateType.NONE;
    }

    public ResponseDto() {
        this.state = StateType.NONE;
        this.message = "";
        this._link = null;
    }

    public StateType getState() { return state; }
    public String getMessage() { return message; }
    public String get_link() { return _link; }

    public void setState(StateType state) { this.state = state; }
    public void setMessage(String message) { this.message = message; }
    public void set_link(String _link) { this._link = _link; }

    @Override
    public String toString(){ return "{\"response\": "+ toJson().toJSONString() + "}"; }

    public JSONObject toJson(){
        // group: response
        JSONObject json = new JSONObject();
        json.put("group", "response");
        json.put("state", state.toString());
        json.put("message", message);
        if( _link != null ) json.put("_link", _link);

        return json;
    }

}
