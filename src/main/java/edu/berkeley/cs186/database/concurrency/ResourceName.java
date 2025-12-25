package edu.berkeley.cs186.database.concurrency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * 这个类表示资源的完整名称。资源的名称是一个有序的字符串元组，
 * 从元组开始的任何子序列都是层级结构中较高资源的名称。
 *
 * 例如，一个页面可能有名称("database", "someTable", 10)，
 * 其中"someTable" 是页面所属表的名称，10 是页码。
 * 我们将其存储为列表 ["database", "someTable", "10"]，
 * 而它在层级结构中的祖先将是 ["database"]（代表整个数据库）和 ["database", "someTable"]
 * （代表表，这是该表的一个页面）。
 */
public class ResourceName {
    private final List<String> names; // 有层级的名称

    public ResourceName(String name) {
        this(Collections.singletonList(name));
    }

    private ResourceName(List<String> names) {
        this.names = new ArrayList<>(names);
    }

    /**
     * @param parent 此资源的父级，如果此资源没有父级则为null
     * @param name 此资源的名称。
     */
    ResourceName(ResourceName parent, String name) {
        this.names = new ArrayList<>(parent.names);
        this.names.add(name);
    }

    /**
     * @return 如果此资源没有父级则返回null，否则返回此资源父级ResourceName的副本。
     */
    ResourceName parent() {
        if (names.size() > 1) {
            return new ResourceName(names.subList(0, names.size() - 1));
        }
        return null;
    }

    /**
     * @return 如果此资源是`other`的后代则返回true，否则返回false
     */
    boolean isDescendantOf(ResourceName other) {
        if (other.names.size() >= names.size()) {
            return false;
        }
        Iterator<String> mine = names.iterator();
        Iterator<String> others = other.names.iterator();
        while (others.hasNext()) {
            if (!mine.next().equals(others.next())) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return 此资源的名称，例如以下列表：
     * - ["database, "someTable", "10"]
     */
    List<String> getNames() {
        return names;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (!(o instanceof ResourceName)) return false;
        ResourceName other = (ResourceName) o;
        if (other.names.size() != this.names.size()) return false;
        for (int i = 0; i < other.names.size(); i++) {
            if (!this.names.get(i).equals(other.names.get(i))) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return names.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder rn = new StringBuilder(names.get(0));
        for (int i = 1; i < names.size(); ++i) {
            rn.append('/').append(names.get(i));
        }
        return rn.toString();
    }
}