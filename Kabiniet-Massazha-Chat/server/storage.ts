import { eq, and, gte, lte, desc } from "drizzle-orm";
import { db } from "./db";
import {
  users,
  bookings,
  chats,
  messages,
  cancellations,
  type User,
  type InsertUser,
  type Booking,
  type InsertBooking,
  type Chat,
  type InsertChat,
  type Message,
  type InsertMessage,
  type Cancellation,
  type InsertCancellation,
} from "@shared/schema";

export interface IStorage {
  getUser(id: string): Promise<User | undefined>;
  getUserByPhone(phone: string): Promise<User | undefined>;
  getClients(): Promise<User[]>;
  createUser(user: InsertUser): Promise<User>;
  
  getBookings(): Promise<Booking[]>;
  getBookingsByDate(date: string): Promise<Booking[]>;
  getBookingsByRoom(roomNumber: number, date: string): Promise<Booking[]>;
  getBookingsByUserId(userId: string): Promise<Booking[]>;
  createBooking(booking: InsertBooking): Promise<Booking>;
  getBooking(id: string): Promise<Booking | undefined>;
  deleteBooking(id: string): Promise<void>;
  updateBookingClient(id: string, userId: string, userName: string, userPhone: string): Promise<void>;
  updateBookingDetails(id: string, roomNumber: number, date: string, startHour: number | null, duration: number | null, bookingType: string, price: number): Promise<void>;
  
  getCancellations(): Promise<Cancellation[]>;
  createCancellation(cancellation: InsertCancellation): Promise<Cancellation>;
  updateCancellationClient(id: string, userId: string, userName: string, userPhone: string): Promise<void>;
  
  getChats(): Promise<Chat[]>;
  getChatByClientId(clientId: string): Promise<Chat | undefined>;
  getChatByPhone(phone: string): Promise<Chat | undefined>;
  createChat(chat: InsertChat): Promise<Chat>;
  updateChatLastMessage(chatId: string, message: string, unreadCount?: number): Promise<void>;
  markChatAsRead(chatId: string): Promise<void>;
  
  getMessagesByChatId(chatId: string): Promise<Message[]>;
  createMessage(message: InsertMessage): Promise<Message>;
}

export class DatabaseStorage implements IStorage {
  async getUser(id: string): Promise<User | undefined> {
    const result = await db.select().from(users).where(eq(users.id, id));
    return result[0];
  }

  async createUser(insertUser: InsertUser): Promise<User> {
    const result = await db.insert(users).values(insertUser).returning();
    return result[0];
  }

  async getUserByPhone(phone: string): Promise<User | undefined> {
    const result = await db.select().from(users).where(eq(users.phone, phone));
    return result[0];
  }

  async getClients(): Promise<User[]> {
    return db.select().from(users).where(eq(users.role, "client"));
  }

  async getBookings(): Promise<Booking[]> {
    return db.select().from(bookings).orderBy(desc(bookings.date), bookings.startHour);
  }

  async getBookingsByDate(date: string): Promise<Booking[]> {
    return db.select().from(bookings).where(eq(bookings.date, date));
  }

  async getBookingsByRoom(roomNumber: number, date: string): Promise<Booking[]> {
    return db
      .select()
      .from(bookings)
      .where(and(eq(bookings.roomNumber, roomNumber), eq(bookings.date, date)));
  }

  async getBookingsByUserId(userId: string): Promise<Booking[]> {
    return db
      .select()
      .from(bookings)
      .where(eq(bookings.userId, userId))
      .orderBy(desc(bookings.date), bookings.startHour);
  }

  async createBooking(insertBooking: InsertBooking): Promise<Booking> {
    const result = await db.insert(bookings).values(insertBooking).returning();
    return result[0];
  }

  async getBooking(id: string): Promise<Booking | undefined> {
    const result = await db.select().from(bookings).where(eq(bookings.id, id));
    return result[0];
  }

  async deleteBooking(id: string): Promise<void> {
    await db.delete(bookings).where(eq(bookings.id, id));
  }

  async updateBookingClient(id: string, userId: string, userName: string, userPhone: string): Promise<void> {
    await db.update(bookings).set({ userId, userName, userPhone }).where(eq(bookings.id, id));
  }

  async updateBookingDetails(id: string, roomNumber: number, date: string, startHour: number | null, duration: number | null, bookingType: string, price: number): Promise<void> {
    await db.update(bookings).set({ roomNumber, date, startHour, duration, bookingType, price }).where(eq(bookings.id, id));
  }

  async markBookingPaid(id: string, paid: boolean, paymentMethod?: string, paidAmount?: number, isDebtor?: boolean, extraTime?: number): Promise<void> {
    const updateData: any = { paid, paymentMethod: paid ? (paymentMethod || null) : null };
    if (paidAmount !== undefined) updateData.paidAmount = paidAmount;
    if (isDebtor !== undefined) updateData.isDebtor = isDebtor;
    if (extraTime !== undefined) updateData.extraTime = extraTime;
    if (!paid) {
      updateData.paidAmount = null;
    }
    await db.update(bookings).set(updateData).where(eq(bookings.id, id));
  }

  async getCancellations(): Promise<Cancellation[]> {
    return db.select().from(cancellations).orderBy(desc(cancellations.cancelledAt));
  }

  async updateCancellationClient(id: string, userId: string, userName: string, userPhone: string): Promise<void> {
    await db.update(cancellations).set({ userId, userName, userPhone }).where(eq(cancellations.id, id));
  }

  async createCancellation(insertCancellation: InsertCancellation): Promise<Cancellation> {
    const result = await db.insert(cancellations).values(insertCancellation).returning();
    return result[0];
  }

  async getChats(): Promise<Chat[]> {
    return db.select().from(chats).orderBy(desc(chats.lastMessageTime));
  }

  async getChatByClientId(clientId: string): Promise<Chat | undefined> {
    const result = await db.select().from(chats).where(eq(chats.clientId, clientId));
    return result[0];
  }

  async getChatByPhone(phone: string): Promise<Chat | undefined> {
    const result = await db.select().from(chats).where(eq(chats.clientPhone, phone));
    return result[0];
  }

  async createChat(insertChat: InsertChat): Promise<Chat> {
    const result = await db.insert(chats).values(insertChat).returning();
    return result[0];
  }

  async updateChatLastMessage(chatId: string, message: string, unreadCount?: number): Promise<void> {
    const updates: Partial<Chat> = {
      lastMessage: message,
      lastMessageTime: new Date(),
    };
    if (unreadCount !== undefined) {
      updates.unreadCount = unreadCount;
    }
    await db.update(chats).set(updates).where(eq(chats.id, chatId));
  }

  async markChatAsRead(chatId: string): Promise<void> {
    await db.update(chats).set({ unreadCount: 0 }).where(eq(chats.id, chatId));
  }

  async getMessagesByChatId(chatId: string): Promise<Message[]> {
    return db.select().from(messages).where(eq(messages.chatId, chatId)).orderBy(messages.timestamp);
  }

  async createMessage(insertMessage: InsertMessage): Promise<Message> {
    const result = await db.insert(messages).values(insertMessage).returning();
    return result[0];
  }
}

export const storage = new DatabaseStorage();
