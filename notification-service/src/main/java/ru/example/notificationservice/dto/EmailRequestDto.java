package ru.example.notificationservice.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

public class EmailRequestDto {

    @NotBlank(message = "Email не должен быть пустым")
    @Email(message = "Некорректный формат Email")
    private String email;

    @NotBlank(message = "Тема письма не должна быть пустой")
    private String subject;

    @NotBlank(message = "Текст письма не должен быть пустым")
    private String text;

    public EmailRequestDto() {}

    public EmailRequestDto(String email, String subject, String text) {
        this.email = email;
        this.subject = subject;
        this.text = text;
    }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    public String getSubject() { return subject; }
    public void setSubject(String subject) { this.subject = subject; }
    public String getText() { return text; }
    public void setText(String text) { this.text = text; }
}
