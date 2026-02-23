import { sql } from "drizzle-orm";
import { pgTable, text, varchar, integer, timestamp, boolean } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

export const users = pgTable("users", {
  id: varchar("id")
    .primaryKey()
    .default(sql`gen_random_uuid()`),
  name: text("name").notNull(),
  phone: text("phone"),
  password: text("password"),
  role: text("role").notNull().default("client"),
  adminUsername: text("admin_username"),
  createdAt: timestamp("created_at").defaultNow(),
});

export const bookings = pgTable("bookings", {
  id: varchar("id")
    .primaryKey()
    .default(sql`gen_random_uuid()`),
  roomNumber: integer("room_number").notNull(),
  userId: varchar("user_id").notNull(),
  userName: text("user_name").notNull(),
  userPhone: text("user_phone"),
  date: text("date").notNull(),
  startHour: integer("start_hour"),
  duration: integer("duration"),
  bookingType: text("booking_type").notNull(),
  price: integer("price").notNull(),
  paid: boolean("paid").default(false),
  paymentMethod: text("payment_method"),
  paidAmount: integer("paid_amount"),
  isDebtor: boolean("is_debtor").default(false),
  extraTime: integer("extra_time").default(0),
  createdAt: timestamp("created_at").defaultNow(),
});

export const chats = pgTable("chats", {
  id: varchar("id")
    .primaryKey()
    .default(sql`gen_random_uuid()`),
  clientId: varchar("client_id").notNull(),
  clientName: text("client_name").notNull(),
  clientPhone: text("client_phone"),
  lastMessage: text("last_message"),
  lastMessageTime: timestamp("last_message_time"),
  unreadCount: integer("unread_count").default(0),
  createdAt: timestamp("created_at").defaultNow(),
});

export const messages = pgTable("messages", {
  id: varchar("id")
    .primaryKey()
    .default(sql`gen_random_uuid()`),
  chatId: varchar("chat_id").notNull(),
  senderId: varchar("sender_id").notNull(),
  senderName: text("sender_name").notNull(),
  senderRole: text("sender_role").notNull(),
  text: text("text").notNull(),
  timestamp: timestamp("timestamp").defaultNow(),
});

export const cancellations = pgTable("cancellations", {
  id: varchar("id")
    .primaryKey()
    .default(sql`gen_random_uuid()`),
  userId: varchar("user_id").notNull(),
  userName: text("user_name").notNull(),
  userPhone: text("user_phone"),
  roomNumber: integer("room_number").notNull(),
  date: text("date").notNull(),
  bookingType: text("booking_type").notNull(),
  price: integer("price").notNull(),
  cancelledAt: timestamp("cancelled_at").defaultNow(),
});

export const insertUserSchema = createInsertSchema(users).omit({ id: true, createdAt: true });
export const insertBookingSchema = createInsertSchema(bookings).omit({ id: true, createdAt: true });
export const insertChatSchema = createInsertSchema(chats).omit({ id: true, createdAt: true });
export const insertMessageSchema = createInsertSchema(messages).omit({ id: true, timestamp: true });
export const insertCancellationSchema = createInsertSchema(cancellations).omit({ id: true, cancelledAt: true });

export type InsertUser = z.infer<typeof insertUserSchema>;
export type User = typeof users.$inferSelect;
export type InsertBooking = z.infer<typeof insertBookingSchema>;
export type Booking = typeof bookings.$inferSelect;
export type InsertChat = z.infer<typeof insertChatSchema>;
export type Chat = typeof chats.$inferSelect;
export type InsertMessage = z.infer<typeof insertMessageSchema>;
export type Message = typeof messages.$inferSelect;
export type InsertCancellation = z.infer<typeof insertCancellationSchema>;
export type Cancellation = typeof cancellations.$inferSelect;
