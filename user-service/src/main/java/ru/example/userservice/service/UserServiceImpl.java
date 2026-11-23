package ru.example.userservice.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.example.userservice.dto.UserEventDto;
import ru.example.userservice.dto.UserRequestDto;
import ru.example.userservice.dto.UserResponseDto;
import ru.example.userservice.exception.UserServiceException;
import ru.example.userservice.model.User;
import ru.example.userservice.repository.UserRepository;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Transactional(readOnly = true)
public class UserServiceImpl implements UserService {

    private final UserRepository userRepository;
    private final KafkaTemplate<String, UserEventDto> kafkaTemplate;
    private static final String TOPIC = "user-events";

    public UserServiceImpl(UserRepository userRepository, KafkaTemplate<String, UserEventDto> kafkaTemplate) {
        this.userRepository = userRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    @Transactional
    public UserResponseDto createUser(UserRequestDto dto) {
        if (dto.getAge() < 18) {
            throw new UserServiceException("Регистрация доступна только с 18 лет.", null);
        }
        if (userRepository.existsByEmail(dto.getEmail())) {
            throw new UserServiceException("Не удалось сохранить пользователя. Возможно, такой email уже зарегистрирован.", null);
        }
        User user = new User(dto.getName(), dto.getEmail(), dto.getAge());
        User savedUser = userRepository.save(user);

        // Отправляем событие о создании в Kafka
        kafkaTemplate.send(TOPIC, new UserEventDto(savedUser.getEmail(), "CREATE"));

        return mapToResponseDto(savedUser);
    }

    @Override
    public UserResponseDto getUserById(Long id) {
        if (id == null || id <= 0) {
            throw new UserServiceException("Некорректный ID пользователя.", null);
        }
        User user = userRepository.findById(id)
                .orElseThrow(() -> new UserServiceException("Ошибка при обращении к базе данных. Попробуйте позже.", null));
        return mapToResponseDto(user);
    }

    @Override
    public List<UserResponseDto> getAllUsers() {
        return userRepository.findAll().stream()
                .map(this::mapToResponseDto)
                .collect(Collectors.toList());
    }

    @Override
    @Transactional
    public UserResponseDto updateUser(Long id, UserRequestDto dto) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new UserServiceException("Не удалось обновить данные. Пользователь не найден.", null));

        user.setName(dto.getName());
        user.setEmail(dto.getEmail());
        user.setAge(dto.getAge());

        return mapToResponseDto(userRepository.save(user));
    }

    @Override
    @Transactional
    public void deleteUser(Long id) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new UserServiceException("Не удалось удалить пользователя из-за внутренней ошибки БД.", null));

        userRepository.delete(user);

        // Отправляем событие об удалении в Kafka
        kafkaTemplate.send(TOPIC, new UserEventDto(user.getEmail(), "DELETE"));
    }

    private UserResponseDto mapToResponseDto(User user) {
        return new UserResponseDto(
                user.getId(),
                user.getName(),
                user.getEmail(),
                user.getAge(),
                user.getCreatedAt()
        );
    }
}
