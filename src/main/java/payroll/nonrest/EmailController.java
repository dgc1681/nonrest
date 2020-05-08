package payroll.nonrest;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@RestController
@RequestMapping(value = "/email")
public class EmailController {
    @Autowired
    public JavaMailSender javaMailSender;
    @GetMapping(value = "/sendMail")
    public String sendMail(){

        Properties props = new Properties();
        props.put("bootstrap.servers","coposoftware.org:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(props);
        try {
            for (int i = 0; i < 150; i++){
                myProducer.send(new ProducerRecord<String, String>("my_topic_prueba",Integer.toString(i),"MyMessage: "+Integer.toString(i)));
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            myProducer.close();
        }

        /*
        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo("diego.gomez@me.com");
        message.setSubject("Prueba de mail");
        message.setText("hoaaaa mundo!!");
        javaMailSender.send(message);*/
        return "Sucessfuly Producer";
    }

    @GetMapping(value = "/consumer")
    public String consumer(){

        Properties props = new Properties();
        props.put("bootstrap.servers","coposoftware.org:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id","test");

        KafkaConsumer myConsumer = new KafkaConsumer(props);
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("my_topic_prueba");
        myConsumer.subscribe(topics);
        try {
            while (true){
                ConsumerRecords<String, String> records = myConsumer.poll(10);
                for (ConsumerRecord<String,String> record : records){
                    System.out.println(
                            String.format("Topic: %s, Partition: %d, Offset: %d, key: %s, Value: %s",
                                    record.topic(), record.partition(), record.offset(), record.key(), record.value())
                    );
                }
            }
        } catch (Exception e){
            System.out.println(e.getMessage());
        } finally {
            myConsumer.close();
        }

        return "Sucessfuly Consumer";
    }
}
