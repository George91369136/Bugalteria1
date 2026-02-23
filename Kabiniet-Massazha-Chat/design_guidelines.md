# Design Guidelines: Massage Room Booking & Messenger App

## Authentication Architecture

**Auth Required**: Yes - dual authentication system

### User Registration (Default View)
- Phone number field (Russian format validation)
- Name field
- Checkbox: "I agree to personal data processing" with link to privacy policy
- Primary CTA: "Register"
- Switch to admin login: Text link "Login as Admin"

### Admin Login (Alternate View)
- Username field (accepts: admin1, admin2, admin3)
- Password field (6319 for all)
- Primary CTA: "Admin Login"
- Switch back: Text link "Register as User"

**Implementation**: Mock authentication with local state. Persist user role (client/admin) to determine UI visibility.

## Navigation Architecture

**Root Navigation**: Tab Bar (3 tabs for clients, 4 tabs for admins)

### Client Tabs:
1. **Booking** (calendar icon) - Book massage rooms
2. **Messages** (message icon) - Chat with admins
3. **Profile** (user icon) - User settings

### Admin Tabs:
1. **Bookings** (calendar icon) - View all reservations
2. **Messages** (message icon) - All client chats
3. **Rooms** (grid icon) - Room status overview
4. **Profile** (user icon) - Admin settings

## Screen Specifications

### 1. Booking Screen (Client)
**Purpose**: Select room, date, and duration to book

**Layout**:
- **Header**: Default navigation header with title "Бронирование" (Booking)
- **Content**: ScrollView with bottom inset = tabBarHeight + Spacing.xl
- **Components**:
  - Room selector: 3 cards showing "Кабинет 1", "Кабинет 2", "Кабинет 3"
  - Booking type toggle: "Почасовая" (Hourly) / "Посуточная" (Daily)
  - Calendar picker (inline, React Native Calendar)
  - Time slot selector (hourly: 10:00-22:00 in 1-hour blocks)
  - Duration selector (for hourly bookings)
  - Price display: Large, prominent text showing total cost
  - Floating CTA button: "Забронировать за [price] ₽" with shadow
  
**Validation**:
- Hourly: max 7 days in advance
- Daily: max 3 months in advance
- Disable already booked slots

### 2. Messages Screen (Client)
**Purpose**: Chat with admins

**Layout**:
- **Header**: Default with title "Сообщения"
- **Content**: Chat interface (not scrollable root, FlatList for messages)
- **Components**:
  - Message list (inverted FlatList)
  - Message bubble (user messages: right-aligned, blue; admin: left-aligned, gray)
  - Input bar (fixed at bottom): TextInput + Send button
  - Bottom inset for input bar = tabBarHeight + Spacing.xl

### 3. Bookings Screen (Admin)
**Purpose**: View all client reservations

**Layout**:
- **Header**: Default with title "Все бронирования"
- **Content**: FlatList with bottom inset = tabBarHeight + Spacing.xl
- **Components**:
  - Filter chips: "Сегодня", "Неделя", "Месяц", "Все"
  - Booking cards showing:
    - Room number
    - Client name & phone
    - Date & time
    - Booking type (hourly/daily)
    - Price
  - Empty state: "Нет бронирований"

### 4. Messages Screen (Admin)
**Purpose**: View and respond to all client chats

**Layout**:
- **Header**: Default with title "Все чаты"
- **Content**: Two-level navigation
  - Chat list (FlatList of clients)
  - Individual chat screen (push navigation)
- **Components**:
  - Chat list items: Client name, last message preview, timestamp, unread badge
  - Chat screen: Same as client chat screen but shows admin name on responses

### 5. Rooms Screen (Admin)
**Purpose**: Quick overview of room availability

**Layout**:
- **Header**: Transparent with title "Кабинеты"
- **Content**: ScrollView with top inset = headerHeight + Spacing.xl, bottom = tabBarHeight + Spacing.xl
- **Components**:
  - 3 large cards (one per room)
  - Each card shows: Room number, current status (available/occupied/booked), next booking time
  - Status indicator: Green dot (available), red dot (occupied), orange dot (booked soon)

### 6. Profile Screen (All Users)
**Purpose**: User settings and account management

**Layout**:
- **Header**: Transparent with title "Профиль"
- **Content**: ScrollView with top inset = headerHeight + Spacing.xl
- **Components**:
  - Avatar (circular, 80pt)
  - Name display
  - Phone number (clients) / Admin ID (admins)
  - Settings list:
    - Notifications toggle
    - Language (Russian default)
    - Theme (Light/Dark)
    - Privacy policy link
    - Terms of service link
  - Logout button (destructive style, bottom of list)
  - Admin-only: "Admin panel accessed as [username]"

## Design System

### Colors
- **Primary**: #4A90E2 (Professional blue)
- **Secondary**: #50C878 (Success green for availability)
- **Accent**: #FF6B6B (Alert red for occupied)
- **Background**: #F8F9FA (Light gray)
- **Surface**: #FFFFFF
- **Text Primary**: #1A1A1A
- **Text Secondary**: #6B7280
- **Border**: #E5E7EB

### Typography
- **Headings**: SF Pro Display (iOS) / Roboto (Android), Bold, 24pt
- **Body**: SF Pro Text / Roboto, Regular, 16pt
- **Caption**: 14pt, Medium
- **Price Display**: 32pt, Bold

### Spacing
- **Tight**: 8pt
- **Normal**: 16pt
- **Loose**: 24pt
- **XL**: 32pt

### Components

**Room Card (Booking)**:
- White surface, 16pt border radius
- Subtle border (1pt, color: Border)
- Pressed state: background tint to Primary with 5% opacity
- Selected state: border color = Primary, border width = 2pt

**Booking Time Slot**:
- Chip style, 12pt border radius
- Available: white with Primary text
- Booked: gray background, disabled
- Selected: Primary background, white text
- Active press feedback required

**Chat Message Bubble**:
- 16pt border radius (asymmetric: reduce radius on sender's bottom corner)
- User messages: Primary background, white text
- Admin messages: #E5E7EB background, Text Primary
- Max width: 80% of screen width
- Timestamp: Caption size, Text Secondary, below bubble

**Floating Book Button**:
- Full-width button at bottom
- Primary background, white text, 50pt height
- Shadow: offset (0, 2), opacity 0.10, radius 2
- Margin: 16pt from screen edges
- Position: bottom inset = tabBarHeight + 16pt

**Price Display (Booking)**:
- Prominent card with light Primary background
- Large bold text: "[price] ₽"
- Subtitle: booking details (e.g., "3 часа • Кабинет 2")

## Critical Assets

1. **Room Icons** (3 unique SVG icons):
   - Minimalist massage table icons differentiated by number badge
   - Style: Line art, 2pt stroke, Primary color

2. **Empty States** (2 illustrations):
   - No bookings: Calendar with checkmark
   - No messages: Chat bubble icon
   - Style: Simple line art matching room icons

3. **Status Indicators**:
   - Colored dots for room availability (Green, Orange, Red)
   - Use system-standard circle shapes, 8pt diameter

## Accessibility
- Minimum touch target: 44pt
- Color contrast ratio: 4.5:1 for text
- VoiceOver labels for all interactive elements
- Haptic feedback on booking confirmation
- Error states with clear messaging in Russian

## Interaction Patterns
- Pull-to-refresh on booking and messages lists
- Swipe actions on admin chat list (mark as read)
- Long-press on booking (admin only) to cancel with confirmation alert
- Loading states for booking submission (show spinner, disable button)
- Success confirmation: Native alert with "Бронирование подтверждено!"