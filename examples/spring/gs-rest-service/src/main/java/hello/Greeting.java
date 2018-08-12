package hello;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
class Greeting {
    private long id;
    private String content;
    private String topic;
    private Long timestamp;
    private String add1;
    private String add2;
    private String add3;
}
