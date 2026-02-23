import type { Express } from "express";
import { createServer, type Server } from "node:http";
import { createHash } from "node:crypto";
import { storage } from "./storage";
import { insertBookingSchema, insertMessageSchema, insertChatSchema, insertUserSchema } from "@shared/schema";

// Simple password hashing using SHA-256 with salt
function hashPassword(password: string): string {
  const salt = "altbody_salt_2024";
  return createHash("sha256").update(password + salt).digest("hex");
}

function normalizePhone(phone: string | null | undefined): string {
  if (!phone) return '';
  let digits = phone.replace(/[^0-9]/g, '');
  if (digits.startsWith('7') && digits.length === 11) {
    digits = digits.substring(1);
  } else if (digits.startsWith('8') && digits.length === 11) {
    digits = digits.substring(1);
  }
  return digits;
}

export async function registerRoutes(app: Express): Promise<Server> {
  // Bookings
  app.get("/api/bookings", async (_req, res) => {
    try {
      const bookings = await storage.getBookings();
      res.json(bookings);
    } catch (error) {
      console.error("Error fetching bookings:", error);
      res.status(500).json({ error: "Failed to fetch bookings" });
    }
  });

  app.get("/api/bookings/user/:userId", async (req, res) => {
    try {
      const bookings = await storage.getBookingsByUserId(req.params.userId);
      res.json(bookings);
    } catch (error) {
      console.error("Error fetching user bookings:", error);
      res.status(500).json({ error: "Failed to fetch user bookings" });
    }
  });

  app.get("/api/bookings/:roomNumber/:date", async (req, res) => {
    try {
      const { roomNumber, date } = req.params;
      const bookings = await storage.getBookingsByRoom(parseInt(roomNumber), date);
      res.json(bookings);
    } catch (error) {
      console.error("Error fetching room bookings:", error);
      res.status(500).json({ error: "Failed to fetch room bookings" });
    }
  });

  app.post("/api/bookings", async (req, res) => {
    try {
      const parsed = insertBookingSchema.safeParse(req.body);
      if (!parsed.success) {
        return res.status(400).json({ error: "Invalid booking data" });
      }
      const data = parsed.data;

      if (data.userPhone) {
        data.userPhone = normalizePhone(data.userPhone);
      }

      if (data.userPhone) {
        const existingUser = await storage.getUserByPhone(data.userPhone);
        if (existingUser) {
          data.userId = existingUser.id;
          data.userName = existingUser.name;
          data.userPhone = normalizePhone(existingUser.phone);
        }
      }

      const existingBookings = await storage.getBookingsByRoom(data.roomNumber, data.date);

      if (data.bookingType === 'daily') {
        if (existingBookings.length > 0) {
          return res.status(409).json({ error: "На этот день уже есть бронирования в этом кабинете. Посуточная бронь невозможна." });
        }
      } else {
        const hasDailyBooking = existingBookings.some(b => b.bookingType === 'daily');
        if (hasDailyBooking) {
          return res.status(409).json({ error: "Кабинет забронирован на весь день. Почасовая бронь невозможна." });
        }
        const newStart = data.startHour;
        const newEnd = (newStart || 0) + (data.duration || 0);
        const overlap = existingBookings.some(b => {
          const bStart = b.startHour || 0;
          const bEnd = bStart + (b.duration || 0);
          return (newStart || 0) < bEnd && newEnd > bStart;
        });
        if (overlap) {
          return res.status(409).json({ error: "Выбранное время пересекается с существующей бронью." });
        }
      }

      const allDateBookings = await storage.getBookingsByDate(data.date);
      const userBookings = allDateBookings.filter(b => b.userId === data.userId || (data.userPhone && b.userPhone === data.userPhone));
      if (userBookings.length > 0) {
        if (data.bookingType === 'daily') {
          const userInOtherRoom = userBookings.some(b => b.roomNumber !== data.roomNumber);
          if (userInOtherRoom) {
            return res.status(409).json({ error: "Этот клиент уже забронирован в другом кабинете на этот день." });
          }
        } else {
          const newStart = data.startHour || 0;
          const newEnd = newStart + (data.duration || 0);
          const clientOverlap = userBookings.some(b => {
            if (b.roomNumber === data.roomNumber) return false;
            if (b.bookingType === 'daily') return true;
            const bStart = b.startHour || 0;
            const bEnd = bStart + (b.duration || 0);
            return newStart < bEnd && newEnd > bStart;
          });
          if (clientOverlap) {
            return res.status(409).json({ error: "Этот клиент уже занят в другом кабинете в это время." });
          }
        }
      }

      const booking = await storage.createBooking(data);
      res.status(201).json(booking);
    } catch (error) {
      console.error("Error creating booking:", error);
      res.status(500).json({ error: "Failed to create booking" });
    }
  });

  app.delete("/api/bookings/:id", async (req, res) => {
    try {
      const booking = await storage.getBooking(req.params.id);
      if (booking) {
        await storage.createCancellation({
          userId: booking.userId,
          userName: booking.userName,
          userPhone: booking.userPhone,
          roomNumber: booking.roomNumber,
          date: booking.date,
          bookingType: booking.bookingType,
          price: booking.price,
        });
      }
      await storage.deleteBooking(req.params.id);
      res.status(204).send();
    } catch (error) {
      console.error("Error deleting booking:", error);
      res.status(500).json({ error: "Failed to delete booking" });
    }
  });

  app.put("/api/bookings/:id/edit", async (req, res) => {
    try {
      const booking = await storage.getBooking(req.params.id);
      if (!booking) {
        return res.status(404).json({ error: "Бронирование не найдено" });
      }

      const { roomNumber, date, startHour, duration, bookingType } = req.body;
      const newRoom = roomNumber || booking.roomNumber;
      const newDate = date || booking.date;
      const newType = bookingType || booking.bookingType;
      let newStart = startHour !== undefined ? startHour : booking.startHour;
      let newDuration = duration !== undefined ? duration : booking.duration;
      let newPrice: number;

      if (newType === 'daily') {
        newStart = 20;
        newDuration = 24;
        newPrice = 3200;
      } else {
        newPrice = (newDuration || 0) * 200;
      }

      const existingBookings = await storage.getBookingsByRoom(newRoom, newDate);
      const otherBookings = existingBookings.filter(b => b.id !== booking.id);

      if (newType === 'daily') {
        if (otherBookings.length > 0) {
          return res.status(409).json({ error: "На этот день уже есть бронирования в этом кабинете." });
        }
      } else {
        const hasDailyBooking = otherBookings.some(b => b.bookingType === 'daily');
        if (hasDailyBooking) {
          return res.status(409).json({ error: "Кабинет забронирован на весь день." });
        }
        const ns = newStart || 0;
        const ne = ns + (newDuration || 0);
        const overlap = otherBookings.some(b => {
          const bStart = b.startHour || 0;
          const bEnd = bStart + (b.duration || 0);
          return ns < bEnd && ne > bStart;
        });
        if (overlap) {
          return res.status(409).json({ error: "Выбранное время пересекается с существующей бронью." });
        }
      }

      const allDateBookings = await storage.getBookingsByDate(newDate);
      const userBookings = allDateBookings.filter(b => b.id !== booking.id && (b.userId === booking.userId || (booking.userPhone && b.userPhone === booking.userPhone)));
      if (userBookings.length > 0) {
        if (newType === 'daily') {
          const userInOtherRoom = userBookings.some(b => b.roomNumber !== newRoom);
          if (userInOtherRoom) {
            return res.status(409).json({ error: "Этот клиент уже забронирован в другом кабинете на этот день." });
          }
        } else {
          const ns = newStart || 0;
          const ne = ns + (newDuration || 0);
          const clientOverlap = userBookings.some(b => {
            if (b.roomNumber === newRoom) return false;
            if (b.bookingType === 'daily') return true;
            const bStart = b.startHour || 0;
            const bEnd = bStart + (b.duration || 0);
            return ns < bEnd && ne > bStart;
          });
          if (clientOverlap) {
            return res.status(409).json({ error: "Этот клиент уже занят в другом кабинете в это время." });
          }
        }
      }

      await storage.updateBookingDetails(req.params.id, newRoom, newDate, newStart, newDuration, newType, newPrice);
      res.status(200).json({ success: true });
    } catch (error) {
      console.error("Error editing booking:", error);
      res.status(500).json({ error: "Failed to edit booking" });
    }
  });

  app.put("/api/bookings/:id/paid", async (req, res) => {
    try {
      const { paid, paymentMethod, paidAmount, isDebtor, extraTime } = req.body;
      await storage.markBookingPaid(req.params.id, paid === true, paymentMethod, paidAmount, isDebtor, extraTime);
      res.status(204).send();
    } catch (error) {
      console.error("Error updating payment status:", error);
      res.status(500).json({ error: "Failed to update payment status" });
    }
  });

  app.get("/api/stats", async (_req, res) => {
    try {
      const [allBookings, allClients, allCancellations] = await Promise.all([
        storage.getBookings(),
        storage.getClients(),
        storage.getCancellations()
      ]);
      res.json({
        bookings: allBookings,
        clients: allClients.map(c => ({ id: c.id, name: c.name, phone: c.phone, createdAt: c.createdAt })),
        cancellations: allCancellations
      });
    } catch (error) {
      console.error("Error fetching stats:", error);
      res.status(500).json({ error: "Failed to fetch stats" });
    }
  });

  app.get("/api/cancellations", async (_req, res) => {
    try {
      const allCancellations = await storage.getCancellations();
      res.json(allCancellations);
    } catch (error) {
      console.error("Error fetching cancellations:", error);
      res.status(500).json({ error: "Failed to fetch cancellations" });
    }
  });

  app.get("/api/activity-log", async (_req, res) => {
    try {
      const [allBookings, allCancellations] = await Promise.all([
        storage.getBookings(),
        storage.getCancellations()
      ]);
      const log: any[] = [];
      for (const b of allBookings) {
        log.push({
          type: 'booking',
          timestamp: b.createdAt,
          userName: b.userName,
          userPhone: b.userPhone,
          roomNumber: b.roomNumber,
          date: b.date,
          bookingType: b.bookingType,
          startHour: b.startHour,
          duration: b.duration,
          price: b.price,
          paid: b.paid,
          paymentMethod: b.paymentMethod,
        });
      }
      for (const c of allCancellations) {
        log.push({
          type: 'cancellation',
          timestamp: c.cancelledAt,
          userName: c.userName,
          userPhone: c.userPhone,
          roomNumber: c.roomNumber,
          date: c.date,
          bookingType: c.bookingType,
          price: c.price,
        });
      }
      log.sort((a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime());
      res.json(log);
    } catch (error) {
      console.error("Error fetching activity log:", error);
      res.status(500).json({ error: "Failed to fetch activity log" });
    }
  });

  // Get all clients (for admin booking)
  app.get("/api/users/clients", async (_req, res) => {
    try {
      const clients = await storage.getClients();
      res.json(clients.map(c => ({ id: c.id, name: c.name, phone: c.phone })));
    } catch (error) {
      console.error("Error fetching clients:", error);
      res.status(500).json({ error: "Failed to fetch clients" });
    }
  });

  // User registration
  app.post("/api/users/register", async (req, res) => {
    try {
      const { name, phone: rawPhone, password } = req.body;
      if (!name || !rawPhone || !password) {
        return res.status(400).json({ error: "Name, phone, and password are required" });
      }
      const phone = normalizePhone(rawPhone);
      
      const existingUser = await storage.getUserByPhone(phone);
      if (existingUser) {
        return res.status(400).json({ error: "User with this phone already exists" });
      }
      
      const user = await storage.createUser({
        name,
        phone,
        password: hashPassword(password),
        role: "client"
      });
      
      // Create a chat for the new user
      await storage.createChat({
        clientId: user.id,
        clientName: name,
        clientPhone: phone
      });
      
      res.status(201).json({
        id: user.id,
        name: user.name,
        phone: user.phone,
        role: user.role
      });
    } catch (error) {
      console.error("Error registering user:", error);
      res.status(500).json({ error: "Failed to register user" });
    }
  });

  // User login
  app.post("/api/users/login", async (req, res) => {
    try {
      const { phone: rawPhone, password } = req.body;
      if (!rawPhone || !password) {
        return res.status(400).json({ error: "Phone and password are required" });
      }
      const phone = normalizePhone(rawPhone);
      
      const user = await storage.getUserByPhone(phone);
      if (!user) {
        return res.status(401).json({ error: "Invalid phone or password" });
      }
      
      if (user.password !== hashPassword(password)) {
        return res.status(401).json({ error: "Invalid phone or password" });
      }
      
      res.json({
        id: user.id,
        name: user.name,
        phone: user.phone,
        role: user.role
      });
    } catch (error) {
      console.error("Error logging in:", error);
      res.status(500).json({ error: "Failed to login" });
    }
  });

  // User lookup by phone (legacy, kept for compatibility)
  app.get("/api/users/phone/:phone", async (req, res) => {
    try {
      const phone = normalizePhone(req.params.phone);
      const user = await storage.getUserByPhone(phone);
      if (user) {
        res.json({
          exists: true,
          id: user.id,
          name: user.name,
          phone: user.phone,
        });
      } else {
        const chat = await storage.getChatByPhone(phone);
        if (chat) {
          res.json({
            exists: true,
            id: chat.clientId,
            name: chat.clientName,
            phone: chat.clientPhone,
          });
        } else {
          res.json({ exists: false });
        }
      }
    } catch (error) {
      console.error("Error looking up user by phone:", error);
      res.status(500).json({ error: "Failed to lookup user" });
    }
  });

  // Chats
  app.get("/api/chats", async (_req, res) => {
    try {
      const chats = await storage.getChats();
      res.json(chats);
    } catch (error) {
      console.error("Error fetching chats:", error);
      res.status(500).json({ error: "Failed to fetch chats" });
    }
  });

  app.post("/api/chats", async (req, res) => {
    try {
      const parsed = insertChatSchema.safeParse(req.body);
      if (!parsed.success) {
        return res.status(400).json({ error: "Invalid chat data" });
      }
      
      const existingChat = await storage.getChatByClientId(parsed.data.clientId);
      if (existingChat) {
        return res.json(existingChat);
      }
      
      const chat = await storage.createChat(parsed.data);
      res.status(201).json(chat);
    } catch (error) {
      console.error("Error creating chat:", error);
      res.status(500).json({ error: "Failed to create chat" });
    }
  });

  app.put("/api/chats/:id/read", async (req, res) => {
    try {
      await storage.markChatAsRead(req.params.id);
      res.status(204).send();
    } catch (error) {
      console.error("Error marking chat as read:", error);
      res.status(500).json({ error: "Failed to mark chat as read" });
    }
  });

  // Messages
  app.get("/api/messages/:chatId", async (req, res) => {
    try {
      const messages = await storage.getMessagesByChatId(req.params.chatId);
      res.json(messages);
    } catch (error) {
      console.error("Error fetching messages:", error);
      res.status(500).json({ error: "Failed to fetch messages" });
    }
  });

  app.post("/api/messages", async (req, res) => {
    try {
      const parsed = insertMessageSchema.safeParse(req.body);
      if (!parsed.success) {
        return res.status(400).json({ error: "Invalid message data" });
      }
      
      const message = await storage.createMessage(parsed.data);
      
      const incrementUnread = parsed.data.senderRole === "client" ? 1 : undefined;
      await storage.updateChatLastMessage(
        parsed.data.chatId,
        parsed.data.text,
        incrementUnread ? (await storage.getChats()).find(c => c.id === parsed.data.chatId)?.unreadCount || 0 + 1 : undefined
      );
      
      res.status(201).json(message);
    } catch (error) {
      console.error("Error creating message:", error);
      res.status(500).json({ error: "Failed to create message" });
    }
  });

  // Get all unique clients from bookings (for merge UI)
  app.get("/api/admin/all-clients", async (req, res) => {
    try {
      const allBookings = await storage.getBookings();
      const allCancellations = await storage.getCancellations();
      const clientMap: Record<string, { userId: string; userName: string; userPhone: string; bookingCount: number }> = {};
      
      for (const b of allBookings) {
        const key = b.userId;
        if (!clientMap[key]) {
          clientMap[key] = { userId: b.userId, userName: b.userName || '', userPhone: b.userPhone || '', bookingCount: 0 };
        }
        clientMap[key].bookingCount++;
        if (b.userPhone && !clientMap[key].userPhone) {
          clientMap[key].userPhone = b.userPhone;
        }
      }
      for (const c of allCancellations) {
        const key = c.userId;
        if (!clientMap[key]) {
          clientMap[key] = { userId: c.userId, userName: c.userName || '', userPhone: c.userPhone || '', bookingCount: 0 };
        }
      }

      res.json(Object.values(clientMap).sort((a, b) => a.userName.localeCompare(b.userName)));
    } catch (error) {
      console.error("Error fetching all clients:", error);
      res.status(500).json({ error: "Failed to fetch clients" });
    }
  });

  // Merge two clients: move all bookings/cancellations from source to target
  app.post("/api/admin/merge-clients", async (req, res) => {
    try {
      const { sourceUserId, targetUserId, targetUserName, targetUserPhone } = req.body;
      if (!sourceUserId || !targetUserId) {
        return res.status(400).json({ error: "sourceUserId and targetUserId required" });
      }

      const allBookings = await storage.getBookings();
      const allCancellations = await storage.getCancellations();
      let updatedCount = 0;

      const phone = normalizePhone(targetUserPhone);

      for (const b of allBookings) {
        if (b.userId === sourceUserId) {
          await storage.updateBookingClient(b.id, targetUserId, targetUserName, phone);
          updatedCount++;
        }
      }
      for (const c of allCancellations) {
        if (c.userId === sourceUserId) {
          await storage.updateCancellationClient(c.id, targetUserId, targetUserName, phone);
          updatedCount++;
        }
      }

      res.json({ success: true, updatedCount });
    } catch (error) {
      console.error("Error merging clients:", error);
      res.status(500).json({ error: "Failed to merge clients" });
    }
  });

  app.post("/api/admin/cleanup-duplicates", async (req, res) => {
    try {
      const allBookings = await storage.getBookings();
      const groups: Record<string, typeof allBookings> = {};
      
      for (const b of allBookings) {
        if (!b.userId?.startsWith('walk_in_')) continue;
        const trimmedName = (b.userName || '').trim().toLowerCase();
        if (!trimmedName) continue;
        if (!groups[trimmedName]) groups[trimmedName] = [];
        groups[trimmedName].push(b);
      }

      let updatedCount = 0;
      for (const [name, bookings] of Object.entries(groups)) {
        if (bookings.length <= 1) continue;
        
        const withPhone = bookings.find(b => b.userPhone && b.userPhone.trim() !== '');
        const canonical = withPhone || bookings[0];
        const canonicalId = canonical.userId;
        const canonicalName = (canonical.userName || '').trim();
        const canonicalPhone = normalizePhone(canonical.userPhone);

        for (const b of bookings) {
          if (b.userId === canonicalId && b.userName === canonicalName && normalizePhone(b.userPhone) === canonicalPhone) continue;
          await storage.updateBookingClient(b.id, canonicalId, canonicalName, canonicalPhone);
          updatedCount++;
        }
      }

      // Also normalize all phone numbers in bookings
      for (const b of allBookings) {
        if (b.userPhone) {
          const norm = normalizePhone(b.userPhone);
          if (norm !== b.userPhone) {
            await storage.updateBookingClient(b.id, b.userId, b.userName, norm);
            updatedCount++;
          }
        }
      }

      // Clean up cancellations too
      const allCancellations = await storage.getCancellations();
      const cancelGroups: Record<string, typeof allCancellations> = {};
      for (const c of allCancellations) {
        if (!c.userId?.startsWith('walk_in_')) continue;
        const trimmedName = (c.userName || '').trim().toLowerCase();
        if (!trimmedName) continue;
        if (!cancelGroups[trimmedName]) cancelGroups[trimmedName] = [];
        cancelGroups[trimmedName].push(c);
      }

      // Check if any cancellation name matches a booking group - use same canonical ID
      for (const [name, cancels] of Object.entries(cancelGroups)) {
        if (groups[name]) {
          const withPhone = groups[name].find(b => b.userPhone && b.userPhone.trim() !== '');
          const canonical = withPhone || groups[name][0];
          for (const c of cancels) {
            if (c.userPhone) {
              const norm = normalizePhone(c.userPhone);
              if (norm !== c.userPhone || c.userId !== canonical.userId) {
                await storage.updateCancellationClient(c.id, canonical.userId, (canonical.userName || '').trim(), normalizePhone(canonical.userPhone));
                updatedCount++;
              }
            } else {
              await storage.updateCancellationClient(c.id, canonical.userId, (canonical.userName || '').trim(), normalizePhone(canonical.userPhone));
              updatedCount++;
            }
          }
        }
      }

      res.json({ success: true, updatedCount });
    } catch (error) {
      console.error("Error cleaning up duplicates:", error);
      res.status(500).json({ error: "Failed to clean up duplicates" });
    }
  });

  const httpServer = createServer(app);
  return httpServer;
}
