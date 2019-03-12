package cp.growth;

import java.util.HashSet;
import java.util.Objects;

public class ItemWithCount  {
    private String value;
    private Integer countLeft;
    private Integer countRight;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Integer getCountLeft() {
        return countLeft;
    }

    public void setCountLeft(Integer countLeft) {
        this.countLeft = countLeft;
    }

    public Integer getCountRight() {
        return countRight;
    }

    public void setCountRight(Integer countRight) {
        this.countRight = countRight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ItemWithCount that = (ItemWithCount) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(countLeft, that.countLeft) &&
                Objects.equals(countRight, that.countRight);
    }

    @Override
    public int hashCode() {

        return Objects.hash(value, countLeft, countRight);
    }

    public static void main(String[] args) {
        ItemWithCount item = new ItemWithCount();
        item.value = "F";
        item.countLeft = 0;
        item.countRight = 4;

        ItemWithCount item2 = new ItemWithCount();
        item2.value = "F";
        item2.countLeft = 0;
        item2.countRight = 4;

        HashSet<ItemWithCount> set = new HashSet<>();
        set.add(item);
        set.add(item2);
        System.out.println(set.size());
    }
}
