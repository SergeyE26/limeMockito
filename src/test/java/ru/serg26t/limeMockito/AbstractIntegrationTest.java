package ru.serg26t.limeMockito;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(classes = LimeMockitoApplication.class)
public abstract class AbstractIntegrationTest {
}
