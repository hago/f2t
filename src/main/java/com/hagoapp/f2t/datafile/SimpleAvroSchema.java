/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t.datafile;

import com.hagoapp.f2t.JsonStringify;

import java.util.ArrayList;
import java.util.List;

public class SimpleAvroSchema implements JsonStringify {
    private String type;
    private String name;
    private String namespace;
    private List<AvroField> fields = new ArrayList<>();

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public List<AvroField> getFields() {
        return fields;
    }

    public void setFields(List<AvroField> fields) {
        this.fields = fields;
    }
}
