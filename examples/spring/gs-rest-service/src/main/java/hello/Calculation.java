package hello;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;


@Data
@NoArgsConstructor
@AllArgsConstructor
@XmlRootElement(name = "calc")
@XmlAccessorType(XmlAccessType.FIELD)
class Calculation {
  @XmlElement
  private Expression expr;
  @XmlElement
  private Double result;

  public Calculation(Expression.Operation operation, List<Double> values, Double result) {
    this.expr = new Expression(operation, values);
    this.result = result;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class Expression {
    @XmlElement
    private Operation operation;
    @XmlElement(name = "val")
    private List<Double> values;

    public enum Operation {
      SUM;
    }
  }
}
