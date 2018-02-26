package hello;

import com.google.gson.Gson;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicLong;

@RestController
@Slf4j
public class GreetingController {
    private final Gson gson = new Gson();
    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();
    private String world = "World";

    @RequestMapping("/greeting")
    public Greeting greeting(@RequestParam(value = "name", defaultValue = "") String name) {
        log.error("Name: '{}'", name);
        if ("".equals(name)) {
            name = world;
        } else {
            try {
                Msg msg = gson.fromJson(name, Msg.class);
                world = "World " + msg.id;
            } catch (Exception ignored) {
            }
        }
        return new Greeting(counter.incrementAndGet(), String.format(template, name));
    }

    @Data
    static class Msg {
        int id;
        String content;
    }
}
