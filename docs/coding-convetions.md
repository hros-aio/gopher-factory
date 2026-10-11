# Coding Conventions
 
## 1. General Principles
 
Code must be boring, explicit, testable, and production-ready.
 
Priorities:
 
1. Correctness
2. Readability
3. Testability
4. Observability
5. Performance
Do not optimize prematurely. Do not hide complexity behind magical abstractions.
 
---
 
## 2. Package Naming
 
Use short, lowercase package names.
 
Good:
 
```text
user
order
repository
postgres
http
kafka
cache
config
```
 
Bad:
 
```text
userService
user_service
UserRepository
utils
common
helpers
```
 
Avoid generic packages like `utils`, `common`, and `shared` unless the content is truly cross-cutting and stable.
 
---
 
## 3. File Naming
 
Use snake_case file names.
 
Examples:
 
```text
user_service.go
user_service_test.go
user_repository.go
user_repository_test.go
user_handler.go
user_handler_test.go
postgres_client.go
kafka_consumer.go
```
 
Rule:
 
```text
Every non-generated .go file should have a matching _test.go file unless explicitly exempted.
```
 
Allowed exemptions:
 
- `main.go`
- generated files
- wire/bootstrap-only files with no logic
- pure type-only files when covered by package tests
Any exemption must be justified in review.
 
---
 
## 4. Layer Naming
 
Use consistent terms:
 
| Layer | Naming |
|---|---|
| Domain entity | `User`, `Order`, `Tenant` |
| Domain interface | `UserRepository`, `EventPublisher` |
| Use case service | `UserService` |
| HTTP handler | `UserHandler` |
| DB model | `UserModel` |
| Request DTO | `CreateUserRequest` |
| Response DTO | `UserResponse` |
| Input object | `CreateUserInput` |
| Output object | `UserOutput` |
 
---
 
## 5. Context Usage
 
Every operation that may block must accept `context.Context`.
 
Required:
 
```go
func (s *UserService) Create(ctx context.Context, input CreateUserInput) (*UserOutput, error)
```
 
Forbidden:
 
```go
func (s *UserService) Create(input CreateUserInput) (*UserOutput, error)
```
 
Context rules:
 
- `ctx` is always the first parameter.
- Do not store context in structs.
- Do not pass nil context.
- Use request context in handlers.
- Use lifecycle context in background workers.
---
 
## 6. Error Handling
 
Use wrapped errors for infrastructure details and domain errors for business decisions.
 
Domain errors:
 
```go
var (
    ErrUserNotFound      = errors.New("user not found")
    ErrUserAlreadyExists = errors.New("user already exists")
)
```
 
Wrap infrastructure errors:
 
```go
return fmt.Errorf("insert user: %w", err)
```
 
Do not compare wrapped errors by string.
 
Use:
 
```go
if errors.Is(err, domain.ErrUserNotFound) {
    // map to 404
}
```
 
Forbidden:
 
```go
if err.Error() == "user not found" {
    // bad
}
```
 
---
 
## 7. HTTP Error Mapping
 
HTTP error mapping must be centralized.
 
Example mapping:
 
| Error | HTTP Status |
|---|---:|
| validation error | 400 |
| unauthenticated | 401 |
| forbidden | 403 |
| not found | 404 |
| conflict | 409 |
| rate limited | 429 |
| unknown internal error | 500 |
 
Handlers should return application/domain errors and let the error middleware map them.
 
---
 
## 8. RESTful API Conventions
 
Use nouns, not verbs.
 
Good:
 
```text
GET    /v1/users
POST   /v1/users
GET    /v1/users/{id}
PATCH  /v1/users/{id}
DELETE /v1/users/{id}
```
 
Bad:
 
```text
POST /v1/createUser
POST /v1/user/update
GET  /v1/getUserById
```
 
Rules:
 
- Use plural resource names.
- Use `GET` for read-only operations.
- Use `POST` for creation and commands.
- Use `PATCH` for partial update.
- Use `PUT` only for full replacement.
- Use `DELETE` for deletion.
- Use `202 Accepted` for async commands.
- Use cursor pagination for large lists.
Pagination format:
 
```json
{
  "data": [],
  "pagination": {
    "limit": 20,
    "next_cursor": "eyJpZCI6..."
  }
}
```
 
---
 
## 9. DTO Rules
 
HTTP DTOs must not be domain entities.
 
Good:
 
```go
type CreateUserRequest struct {
    Email string `json:"email" validate:"required,email"`
    Name  string `json:"name" validate:"required,min=2,max=100"`
}
```
 
Use case input:
 
```go
type CreateUserInput struct {
    Email string
    Name  string
}
```
 
Domain:
 
```go
type User struct {
    ID    UserID
    Email Email
    Name  string
}
```
 
Rules:
 
- Request/response DTOs belong to adapter/http.
- Input/output objects belong to application layer.
- Domain entities belong to domain layer.
- DB models belong to infrastructure layer.
---
 
## 10. Validation Rules
 
Validate request shape at HTTP boundary.
Validate business rules inside application/domain layer.
 
Examples:
 
HTTP boundary validation:
 
```text
email is required
email must be valid format
name max length is 100
```
 
Business validation:
 
```text
email must be unique
user cannot be activated before email verification
tenant cannot exceed plan limit
```
 
---
 
## 11. GORM Conventions
 
GORM code must stay inside infrastructure repository packages.
 
DB model example:
 
```go
type UserModel struct {
    ID        string         `gorm:"type:uuid;primaryKey"`
    Email     string         `gorm:"type:varchar(255);uniqueIndex;not null"`
    Name      string         `gorm:"type:varchar(100);not null"`
    CreatedAt time.Time      `gorm:"not null"`
    UpdatedAt time.Time      `gorm:"not null"`
    DeletedAt gorm.DeletedAt `gorm:"index"`
}
 
func (UserModel) TableName() string {
    return "users"
}
```
 
Rules:
 
- Define `TableName()` explicitly.
- Use explicit column types for important fields.
- Do not expose `gorm.DB` outside infrastructure.
- Do not use `AutoMigrate` in production runtime.
- Do not mix repository and use case logic.
---
 
## 12. Transaction Conventions
 
Application layer controls transaction boundaries through an interface.
 
```go
type TxManager interface {
    WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}
```
 
Use case example:
 
```go
func (s *UserService) Create(ctx context.Context, input CreateUserInput) (*UserOutput, error) {
    var output *UserOutput
 
    err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
        user, err := domain.NewUser(input.Email, input.Name)
        if err != nil {
            return err
        }
 
        if err := s.users.Save(ctx, user); err != nil {
            return err
        }
 
        output = ToUserOutput(user)
        return nil
    })
    if err != nil {
        return nil, err
    }
 
    return output, nil
}
```
 
---
 
## 13. Redis Conventions
 
Cache key format:
 
```text
<service>:<module>:<resource>:<id>
```
 
Example:
 
```go
func UserProfileKey(userID string) string {
    return "user-svc:user:profile:" + userID
}
```
 
Rules:
 
- Define key builders as functions.
- Always set TTL unless there is a documented reason.
- Cache miss must not be an error in business logic.
- Redis failure must degrade gracefully unless Redis is required for correctness.
---
 
## 14. Kafka Conventions
 
Topic naming:
 
```text
<domain>.<event-name>.v<version>
```
 
Examples:
 
```text
user.created.v1
tenant.created.v1
subscription.activated.v1
```
 
Message key:
 
```text
<tenant_id>:<aggregate_id>
```
 
Rules:
 
- All messages must use event envelope.
- All consumers must be idempotent.
- Use correlation ID for tracing.
- Do not publish Kafka events directly from HTTP handlers.
- Publish from application service or outbox processor.
---
 
## 15. Slog Conventions
 
Use structured attributes.
 
Good:
 
```go
logger.InfoContext(ctx, "user created",
    slog.String("user_id", user.ID.String()),
    slog.String("email", user.Email.String()),
)
```
 
Bad:
 
```go
logger.Infof("user %s created", user.ID)
```
 
Required fields when available:
 
```text
trace_id
request_id
tenant_id
user_id
correlation_id
```
 
---
 
## 16. Fx Conventions
 
Fx must only wire dependencies.
 
Constructor style:
 
```go
type UserServiceParams struct {
    fx.In
 
    Users domain.UserRepository
    Tx    application.TxManager
    Log   *slog.Logger
}
 
func NewUserService(p UserServiceParams) *UserService {
    return &UserService{
        users: p.Users,
        tx:    p.Tx,
        log:   p.Log,
    }
}
```
 
Rules:
 
- Do not call `fx.New` outside bootstrap or tests.
- Do not inject `fx.Lifecycle` into business services.
- Use `fx.Module` per bounded context.
- Use `fx.Invoke` only for registration/startup tasks.
---
 
## 17. Test Naming
 
Use table-driven tests.
 
```go
func TestUserService_Create(t *testing.T) {
    tests := []struct {
        name    string
        input   CreateUserInput
        mock    func(*Mocks)
        wantErr error
    }{
        // cases
    }
 
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // arrange, act, assert
        })
    }
}
```
 
Rules:
 
- Every test name must describe behavior.
- Use `require` for setup assertions.
- Use `assert` for result assertions.
- Do not depend on test order.
- Do not use real external services in unit tests.
---
 
## 18. Comments
 
Comment exported identifiers.
 
Good:
 
```go
// UserService handles user application use cases.
type UserService struct {
    // ...
}
```
 
Avoid obvious comments:
 
```go
// Create creates a user.
func Create() {}
```
 
Comment why, not what.