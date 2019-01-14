package net.bitnine.agensbrowser.web.persistence.outer.model;

import net.bitnine.agensbrowser.web.persistence.outer.model.type.LabelType;

import net.bitnine.agensbrowser.web.util.JsonbUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NodeType extends ElementType implements Serializable, Cloneable {

    private static final long serialVersionUID = -9063462726499884789L;

    private static final Pattern nodePattern;
    static {
        nodePattern = Pattern.compile("(.+?)\\[(.+?)\\](.*)");
    }

    public NodeType() {
        super();
        this.group = "nodes";
    }
    public NodeType(String label) {
        super(label);
        this.group = "nodes";
    }
    public NodeType(LabelType label) {
        super(label);
        this.group = "nodes";
    }
    public NodeType(Vertex vertex) {
        super((Element) vertex);
        this.group = "nodes";
    }

    public void setValue(String value) throws SQLException {
        Matcher m = nodePattern.matcher(value);
        if (m.find()) {
            label = m.group(1).trim();
            id = m.group(2).trim();
            props = JsonbUtil.parseJsonToMap(m.group(3));
        } else {
            throw new PSQLException("Parsing NODE failed", PSQLState.DATA_ERROR);
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        NodeType node = (NodeType) super.clone();
        return node;
    }

}

/*
"keyword[6.5084]{\"id\": 1, \"keyword\": \"number-in-title\", \"phonetic_code\": \"N5165\"}"

** 프로퍼티 파싱에서 오류 발생!!
org.postgresql.util.PSQLException: Parsing properties of NODE failed
==>
"production[4.161857]{
\"id\": 2,
\"kind\": \"tv series\",
\"title\": \"#1 Single\",
\"md5sum\": \"e424d95c3d5c10fe98eb923a9b05d8da\",
\"full_info\": [{\"certificates\": \"USA:TV-PG\"}, {\"color info\": \"Color\"}, {\"countries\": \"USA\"}, {\"genres\": \"Reality-TV\"}, {\"languages\": \"English\"}, {\"locations\": \"New York City, New York, USA\"}, {\"runtimes\": \"30\"}, {\"sound mix\": \"Stereo\"}, {\"release dates\": \"USA:22 January 2006\"}],
\"series_years\": \"2006-????\",
\"phonetic_code\": \"S524\",
\"production_year\": 2006
}"

"person[3.3]{\"id\": 887, \"name\": \"Aaberge, Theodor Olai\", \"gender\": \"m\", \"md5sum\": \"eef277cb705ce78a3b41ed22b5d56292\", \"name_pcode_cf\": \"A1623\", \"name_pcode_nf\": \"T3641\", \"surname_pcode\": \"A162\"}"
 */