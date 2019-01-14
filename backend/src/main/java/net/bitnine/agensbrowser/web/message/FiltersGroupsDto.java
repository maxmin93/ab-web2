package net.bitnine.agensbrowser.web.message;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
http://localhost:8085/api/graph/filterby-groupby/1001
@RequestBody
{
	"filters" : {
		"customer" : [
				["city", "eq", "Paris"],
				["genger", "eq", "male"],
				["age", "gt", 40]
			],
		"order" : [
			["employeeid", "neq", "8"]
		]
	},
	"groups": {
		"customer" : ["country", "city"],
		"order": ["employeeid"]
	}
}

 */

public class FiltersGroupsDto {

    Map<String, List<List<Object>>> filters;
    Map<String, List<String>> groups;

    public FiltersGroupsDto() {
        this.filters = new HashMap<String, List<List<Object>>>();
        this.groups = new HashMap<String, List<String>>();
    }

    public FiltersGroupsDto(Map<String, List<List<Object>>> filters, Map<String, List<String>> groups) {
        this.filters = filters;
        this.groups = groups;
    }

    public Map<String, List<List<Object>>> getFilters() {
        return filters;
    }
    public void setFilters(Map<String, List<List<Object>>> filters) {
        this.filters = filters;
    }

    public Map<String, List<String>> getGroups() {
        return groups;
    }
    public void setGroups(Map<String, List<String>> groups) {
        this.groups = groups;
    }

    @Override
    public String toString() {
        return "FiltersGroupsDto{" +
                "filters=" + filters.toString() +
                ", groups=" + groups.toString() +
                '}';
    }
}
