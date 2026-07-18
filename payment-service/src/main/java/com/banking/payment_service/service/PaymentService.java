package com.banking.payment_service.service;

import com.banking.payment_service.dto.CreatePaymentRequest;
import com.banking.payment_service.dto.PaymentOrderResponse;
import com.banking.payment_service.entity.Payment;
import com.banking.payment_service.entity.PaymentStatus;
import com.banking.payment_service.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String,Object> kafkaTemplate;

    @Value("${razor.key-id}")
    private String keyId;

    @Value("${razor.key-secret}")
    private String keySecret;

    private static final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private static final String PAYMENT_FAILED_TOPIC = "payment.failed";

    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) throws RazorpayException {
        log.info("Creating payment order for account: {} amount: {}",
                request.getAccountNumber(),
                request.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId,keySecret);

        int convertedAmount = request.getAmount().multiply(BigDecimal.valueOf(100)).intValue();

        JSONObject orderRequest = new JSONObject();
        orderRequest.put("amount",convertedAmount);
        orderRequest.put("currency","USD");
        orderRequest.put("receipt","rcpt_" + System.currentTimeMillis() + UUID.randomUUID().
                toString()
                .replace("-","")
                .substring(0,10)
        );

        Order razorpayOrder = razorpayClient.orders.create(orderRequest);
        log.info("Razorpay order created: {}",razorpayOrder.get("id").toString());

        Payment payment = new Payment();
        payment.setRazorpayOrderId(razorpayOrder.get("id").toString());
        payment.setAmount(request.getAmount());
        payment.setAccountNumber(request.getAccountNumber());
        payment.setCurrency("USD");
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(request.getDescription());

        Payment savedPayment = paymentRepository.save(payment);

        return new PaymentOrderResponse(
                savedPayment.getId(),
                razorpayOrder.get("id").toString(),
                request.getAmount(),
                "USD",
                "CREATED",
                keyId
        );

    }

    public void handleWebhook(Map<String,Object> payload){
        log.info("Received webhook event: {}",payload.get("event"));
        String event = (String) payload.get("event");

        if("payment.captured".equals(event)){
            handlePaymentSuccess(payload);
        }
        else if("payment.failed".equals(event)){
            handlePaymentFailure(payload);
        }

    }

    public void handlePaymentSuccess(Map<String,Object> payload){
        try{
            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order-id");
            String paymentId = (String) paymentData.get("id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(()->new RuntimeException("Payment not found for orderId: " + orderId));

            payment.setRazorpayOrderId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);
            paymentRepository.save(payment);

            Map<String,Object> event = new HashMap<>();
            event.put("paymentId",payment.getId());
            event.put("accountNumber",payment.getAccountNumber());
            event.put("amount",payment.getAmount());
            event.put("razorpayPaymentId",paymentId);

            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC,paymentId,event);
            log.info("Payment completed: {}",paymentId);
        }
        catch(Exception e){
            log.error("Error handling payment success: {}",e.getMessage());
        }
    }

    public void handlePaymentFailure(Map<String,Object> payload){
        try{
            Map<String,Object> paymentData = extractPaymentData(payload);
            String orderId = (String) paymentData.get("order-id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId)
                    .orElseThrow(()->new RuntimeException("Payment not found for orderId: " + orderId));

            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason("Payment failed via Razorpay");
            paymentRepository.save(payment);

            Map<String,Object> event = new HashMap<>();
            event.put("accountNumber",payment.getAccountNumber());
            event.put("amount",payment.getAmount());
            event.put("reason","Payment failed via Razorpay");

            kafkaTemplate.send(PAYMENT_FAILED_TOPIC,payment.getId(),event);
            log.warn("Payment failed: {}",payment.getId());
        }
        catch(Exception e){
            log.error("Error handling payment failure: {}",e.getMessage());
        }
    }

    //to remove nested json layers by casting
    public Map<String,Object> extractPaymentData(Map<String,Object> payload){
        Map<String,Object> entity = (Map<String, Object>) payload.get("payload");

        Map<String,Object> paymentWrapper = (Map<String, Object>) entity.get("payment");

        return (Map<String, Object>) paymentWrapper.get("entity");
    }


}
