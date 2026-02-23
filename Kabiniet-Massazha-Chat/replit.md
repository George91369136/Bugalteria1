# Altbody - Massage Room Booking & Messenger

## Overview

A web application for booking massage rooms with integrated client-admin messaging. The app supports dual user roles (clients and admins) with role-specific interfaces. Clients can book one of three massage rooms (hourly or daily), while admins manage bookings, view room status, and communicate with clients.

**Primary Language**: Russian (UI text and user-facing content)

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture
- **Framework**: React 18 (loaded via CDN)
- **Rendering**: Single-page application served as HTML from Express
- **State Management**: React Context for auth state, useState for local state
- **Styling**: Inline CSS with modern design (dark theme, glass morphism effects)
- **Local Storage**: localStorage for session persistence

### Authentication System
- **Dual Auth Model**: Clients register with phone/name/password; admins login with predefined credentials
- **Admin Credentials**: Usernames `admin1`, `admin2`, `admin3` with shared password `6319` (client-side check only)
- **Client Auth**: Password-based registration/login. Passwords hashed with SHA-256 + salt.
- **State Persistence**: User session stored in localStorage

### Navigation Structure
- **Client Tabs**: Booking, Messages, Profile (3 tabs)
- **Admin Tabs**: Bookings (Брони), Chats (Чаты), Payments (Оплата), Statistics (Стат.), Log (Лог), Cancellations (Отмены), Profile (Профиль) (7 tabs)

### Backend Architecture
- **Runtime**: Express.js with TypeScript (tsx for development)
- **API Pattern**: RESTful routes prefixed with `/api`
- **Storage Layer**: PostgreSQL with Drizzle ORM (DatabaseStorage class)
- **Server Port**: 5000

### Booking System
- **Time encoding**: `startHour` stores time as half-hours from midnight (10:00=20, 10:30=21, 11:00=22, ..., 21:30=43)
- **Duration**: Stored as number of 30-minute blocks (2=1 hour, 3=1.5 hours, etc.)
- **Hourly price**: 200 rubles per 30-minute block (400 rubles/hour)
- **Daily price**: 3200 rubles for full day (10:00-22:00)
- **Minimum booking**: 1 hour (2 blocks)
- **Hourly advance**: 30 days
- **Daily advance**: 90 days
- **Daily encoding**: startHour=20, duration=24
- **Admin can create bookings**: For existing registered clients (select from list) or new walk-in clients (enter name/phone manually)
- **Admin color-coding**: Different clients' bookings shown in distinct colors (from CLIENT_COLORS palette)
- **Admin day detail**: Admin can click into any day to see detailed bookings for all 3 rooms, color-coded by client
- **Conflict validation**: Server rejects overlapping hourly bookings, mutual exclusion between daily and hourly in same room/date
- **Admin booking edit**: Admin can edit room, date, time, duration of existing bookings via "Редактировать" button in booking details. Uses PUT /api/bookings/:id/edit with full conflict validation.

### Payment System
- **Paid field**: Boolean on bookings table, default false
- **Payment method**: `paymentMethod` field stores 'cash' or 'card', cleared when marking unpaid
- **Admin workflow**: Click "Оплатить" → choose Наличные/Карта → booking marked paid with method
- **Admin toggle**: Admin can mark bookings back to unpaid (clears payment method)
- **Overdue detection**: Unpaid bookings whose time has passed are highlighted red
- **Partial payment**: Admin can enter custom paid amount; if less than total, client auto-marked as debtor
- **Debtor tracking**: Manual debtor checkbox on each booking; "Должники" filter and counter in summary
- **Extra time**: Admin can add extra 30-min blocks (200₽ each) that add to total price
- **Filters**: All, Unpaid, Paid, Overdue, Debtors
- **API**: PUT /api/bookings/:id/paid accepts {paid: boolean, paymentMethod?: 'cash'|'card', paidAmount?: number, isDebtor?: boolean, extraTime?: number}

### Statistics Dashboard
- **Period filters**: День (day picker), Неделя, Месяц, 3 мес., Год, Период (custom date range)
- **Day picker**: Select specific date to view stats for that day
- **Custom period**: Select from/to dates for arbitrary date range
- **Comparison mode**: Toggle to compare current period vs previous equivalent period, shows % change arrows
- **Summary cards**: New clients, total bookings, cancellations count (with comparison arrows)
- **Bookings card**: Total bookings sum, count, and upcoming (not yet passed) amount
- **Revenue card**: Only past/completed bookings count as revenue; shows total revenue, paid, unpaid
- **Payment method breakdown**: Cash vs Card revenue and payment count (based on past bookings only)
- **Line charts**: Client registrations, bookings sum, and revenue over time (hidden in day view)
- **Overall stats**: Total clients, bookings breakdown (hourly/daily), average check

### Data Models
- **Bookings**: Room selection (1-3), hourly (200 rubles/30min) or daily (3200 rubles) booking types, paid status
- **Messages/Chats**: Real-time messaging between clients and admins
- **Users**: Identified by phone number (clients) or username (admins)
- **Cancellations**: Tracks cancelled bookings with user info, room, date, price

### API Endpoints
- `GET /api/bookings` - Get all bookings
- `GET /api/bookings/user/:userId` - Get user's bookings
- `GET /api/bookings/:roomNumber/:date` - Get room bookings for date
- `POST /api/bookings` - Create booking
- `DELETE /api/bookings/:id` - Cancel booking
- `PUT /api/bookings/:id/edit` - Edit booking details (body: {roomNumber?, date?, startHour?, duration?, bookingType?})
- `PUT /api/bookings/:id/paid` - Toggle payment status (body: {paid: boolean})
- `GET /api/stats` - Get statistics (bookings, clients, cancellations)
- `GET /api/cancellations` - Get all cancellations
- `GET /api/activity-log` - Combined chronological log of bookings and cancellations
- `GET /api/users/clients` - List all registered clients (for admin booking)
- `POST /api/users/register` - Register new client (name, phone, password)
- `POST /api/users/login` - Login client (phone, password)
- `GET /api/users/phone/:phone` - Lookup user by phone
- `GET /api/chats` - Get all chats
- `POST /api/chats` - Create chat
- `PUT /api/chats/:id/read` - Mark chat as read
- `GET /api/messages/:chatId` - Get chat messages
- `POST /api/messages` - Send message

## Key Files
- `server/index.ts` - Express server setup, serves web app
- `server/routes.ts` - API route definitions
- `server/storage.ts` - Database storage layer
- `server/templates/index.html` - Client React web application
- `server/templates/admin.html` - Admin panel React application
- `shared/schema.ts` - Database schema (Drizzle ORM)

## Environment Variables
- `REPLIT_DEV_DOMAIN`: Development server domain
- `REPLIT_DOMAINS`: Production domains for CORS
