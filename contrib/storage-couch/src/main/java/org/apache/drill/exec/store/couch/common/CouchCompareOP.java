
package org.apache.drill.exec.store.couch.common;

public enum  CouchCompareOP {
    EQUAL("$eq"), NOT_EQUAL("$ne"), GREATER_OR_EQUAL("$gte"), GREATER("$gt"), LESS_OR_EQUAL(
            "$lte"), LESS("$lt"), IN("$in"), AND("$and"), OR("$or"), REGEX("$regex"), OPTIONS(
            "$options"), PROJECT("$project"), COND("$cond"), IFNULL("$ifNull"), IFNOTNULL(
            "$ifNotNull"), SUM("$sum"), GROUP_BY("$group"), EXISTS("$exists");
    private String compareOp;

    CouchCompareOP(String compareOp) {
        this.compareOp = compareOp;
    }

    public String getCompareOp() {
        return compareOp;
    }
}
