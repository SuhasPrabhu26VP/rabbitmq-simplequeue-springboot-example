package com.message_broker.rabbitmq_producer.config;




import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Configuration
public class RabbitMqProducerConfig {

    @Value("${spring.rabbitmq.queueName}")
    private String queueName;

    @Value("${spring.rabbitmq.exchangeName}")
    private String directExchangeName;

    @Value("${spring.rabbitmq.routingKey}")
    private String routingKey;

    @Autowired
    private ProducerConfigProperties producerConfigProperties;


    @Bean
    public Queue queue(){
        return new Queue(queueName);
    }

    @Bean
    public DirectExchange exchange(){
        return new DirectExchange(directExchangeName);
    }


    @Bean
    public Binding binding(){
        return BindingBuilder.bind(queue()).to(exchange()).with(routingKey);
    }

    @Bean
    public MessageConverter converter(){return new JacksonJsonMessageConverter();}

    @Bean
    public Declarables fanoutBindings(){
        final Map<String, ProducerConfigProperties.FanoutQueueProperties> queueHeaderConfigMap = producerConfigProperties.getFanout();
        final List<QueueHeaderConfig> configs = queueHeaderConfigMap.entrySet().stream().map(entry->toQueueHeaderConfig(entry.getValue())).toList();
        final FanoutExchange fanoutExchange = ExchangeBuilder.fanoutExchange(configs.get(0).exchangeName()).build();
        final List<Declarable> declarables = new ArrayList<>();
        declarables.add(fanoutExchange);

        configs.forEach(config -> {
            Queue queue1 = new Queue(config.queueName(), config.durable(),config.exclusive(), config.autoDelete());
            declarables.add(queue1);
            declarables.add(BindingBuilder.bind(queue1).to(fanoutExchange));
        });

        return new Declarables(declarables);
    }

    @Bean
    public Declarables topicBindings() {
        final Map<String, ProducerConfigProperties.TopicQueueProperties> queueHeaderConfigMap = producerConfigProperties.getTopic();
        final List<QueueHeaderConfig> configs = queueHeaderConfigMap.entrySet().stream().map(entry->toQueueHeaderConfig(entry.getValue())).toList();
        final TopicExchange topicExchange = ExchangeBuilder.topicExchange(configs.get(0).exchangeName()).build();
        final List<Declarable> declarables = new ArrayList<>();
        declarables.add(topicExchange);

        configs.forEach(config -> {
            Queue queue1 = new Queue(config.queueName(), config.durable(),config.exclusive(), config.autoDelete());
            declarables.add(queue1);
            declarables.add(BindingBuilder.bind(queue1).to(topicExchange).with(config.routingKey()));
        });

        return new Declarables(declarables);
    }

    @Bean
    public Declarables headerBindings() {

        final Map<String, ProducerConfigProperties.HeaderQueueProperties> queueHeaderConfigMap = producerConfigProperties.getHeader();
        final List<QueueHeaderConfig> configs = queueHeaderConfigMap.entrySet().stream().map(entry->toQueueHeaderConfig(entry.getValue())).toList();
        final HeadersExchange headersExchange = ExchangeBuilder.headersExchange(configs.get(0).exchangeName()).build();
        final List<Declarable> declarables = new ArrayList<>();
        declarables.add(headersExchange);

        configs.forEach(config -> {
            Queue queue2 = new Queue(config.queueName(), config.durable(),config.exclusive(), config.autoDelete());
            declarables.add(queue2);
            declarables.add(buildHeaderBinding(queue2, headersExchange, config));
        });

        return new Declarables(declarables);
    }



    private Binding buildHeaderBinding(Queue queue, HeadersExchange exchange, QueueHeaderConfig config) {
        return switch (config.strategy()) {
            case ALL    -> BindingBuilder.bind(queue).to(exchange).whereAll(config.region(),config.priority()).exist();
            case ANY    -> BindingBuilder.bind(queue).to(exchange).whereAny(config.region(),config.priority()).exist();
            case SINGLE -> BindingBuilder.bind(queue).to(exchange).where(config.region()).exists();
        };
    }
    private QueueHeaderConfig toQueueHeaderConfig(ProducerConfigProperties.BaseQueueProperties props) {
        final QueueHeaderConfig queueHeaderConfig;
        if(props instanceof ProducerConfigProperties.HeaderQueueProperties headerQueueProperties){
            return new QueueHeaderConfig(
                    headerQueueProperties.getQueueName(),
                    headerQueueProperties.getExchangeName(),
                    null,
                    headerQueueProperties.getRegion(),
                    headerQueueProperties.getPriority(),
                    StringUtils.isNotBlank(headerQueueProperties.getStrategy())?HeaderMatchStrategy.valueOf(headerQueueProperties.getStrategy()):null,
                    props.getDurable(),
                    props.getExclusive(),
                    props.getAutoDelete()
            );
        }
        if(props instanceof ProducerConfigProperties.TopicQueueProperties topicQueueProperties){
            return new QueueHeaderConfig(
                    topicQueueProperties.getQueueName(),
                    topicQueueProperties.getExchangeName(),
                    topicQueueProperties.getRoutingKey(),
                    null,
                    null,
                    null,
                    topicQueueProperties.getDurable(),
                    topicQueueProperties.getExclusive(),
                    topicQueueProperties.getAutoDelete()
            );
        }
        return new QueueHeaderConfig(
                props.getQueueName(),
                props.getExchangeName(),
               null,
                null,
                null,
                null,
                props.getDurable(),
                props.getExclusive(),
                props.getAutoDelete());
    }
}
