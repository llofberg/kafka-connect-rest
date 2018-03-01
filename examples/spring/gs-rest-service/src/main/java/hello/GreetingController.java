package hello;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicLong;

@RestController
@Slf4j
public class GreetingController {
    private static final String template = "Hello, %s! (%d)";
    private final AtomicLong counter = new AtomicLong();
    private long count = 0L;

    @RequestMapping("/greeting")
    public Greeting greeting(@RequestParam(value = "name", defaultValue = "World") String name) {
        return new Greeting(counter.incrementAndGet(), String.format(template, name, count));
    }

    @PostMapping(value = "/count")
    public void count(@RequestBody Greeting greeting) {
        log.info("Greeting: '{}'", greeting);
        this.count = greeting.getId();
    }
}
