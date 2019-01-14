package net.bitnine.agensbrowser.web.message;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PropStatDto extends ResponseDto implements Serializable {

    private static final long serialVersionUID = -3648282421110299383L;

    private String group = "prop_stat";
    private Long gid = -1L;
    private String label = null, prop = null, type = null;

    // DescriptiveStatistics
    DescStat stat = new DescStat();
    // Frequency Distribution
    private List<PropFreq> rows = new ArrayList<PropFreq>();    // [ {val, freq, missing}, ... ]

    public PropStatDto(){
        super();
    }
    public PropStatDto(Long gid, String label, String prop){
        super();
        this.gid = gid;
        this.label = label;
        this.prop = prop;
        this.type = type;
    }

    public void setType(String type){ this.type = type; }
    public void setStat(DescriptiveStatistics stat){ this.stat = new DescStat(stat); }
    public void setRows(List<PropFreq> rows){ this.rows = rows; }

    @Override
    public String toString(){ return "{\"prop_stat\": "+ toJson().toJSONString() + "}"; }

    public JSONObject toJson(){
        JSONObject json = new JSONObject();
        json.put("group", group);
        json.put("state", state.toString());
        json.put("message", message);

        json.put("gid", gid);
        json.put("label", label);
        json.put("prop", prop);
        json.put("type", type);

        json.put("stat", stat);

        JSONArray jsonArray = new JSONArray();
        for( PropFreq item: rows ) jsonArray.add( item );
        json.put("rows", jsonArray);

        return json;
    }

    public static class PropFreq {
        private String value = "";
        private Long freq = 0L;
        private Boolean missing = true;

        public PropFreq() {}
        public PropFreq(String value, Long freq, Boolean missing) {
            this.value = value;
            this.freq = freq;
            this.missing = missing;
        }

        public String getValue() { return value; }
        public Long getFreq() { return freq; }
        public Boolean isMissing() { return missing; }
    }

    public static class DescStat {
        private Double n = new Double(0);
        private Double min = new Double(0), max = new Double(0);
        private Double mean = new Double(0), median = new Double(0);
        private Double stdev = new Double(0), skew = new Double(0), kurt = new Double(0);

        public DescStat(){}
        public DescStat(DescriptiveStatistics stat){
            this.n = Double.valueOf(stat.getN());
            this.min = Double.valueOf(stat.getMin());
            this.max = Double.valueOf(stat.getMax());
            this.mean = Double.valueOf(stat.getMean());
            this.median = Double.valueOf(stat.getPercentile(50));
            this.stdev = Double.valueOf(stat.getStandardDeviation());
            this.skew = Double.valueOf(stat.getSkewness());
            this.kurt = Double.valueOf(stat.getKurtosis());
        }

        public Double getN() { return n; }
        public Double getMin() { return min; }
        public Double getMax() { return max; }
        public Double getMean() { return mean; }
        public Double getMedian() { return median; }
        public Double getStdev() { return stdev; }
        public Double getSkew() { return skew; }
        public Double getKurt() { return kurt; }
    }

}
