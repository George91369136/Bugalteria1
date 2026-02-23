import React, { useEffect } from 'react';
import { View, Text, StyleSheet, Linking, Platform } from 'react-native';

export default function App() {
  useEffect(() => {
    if (Platform.OS === 'web') {
      const currentUrl = window.location.href;
      const domain = window.location.hostname;
      const redirectUrl = `https://${domain}:5000/`;
      window.location.href = redirectUrl;
    }
  }, []);

  return (
    <View style={styles.container}>
      <Text style={styles.title}>Altbody</Text>
      <Text style={styles.subtitle}>Перенаправление на сайт...</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: '#1a1a2e',
  },
  title: {
    fontSize: 32,
    fontWeight: 'bold',
    color: '#4A90E2',
    marginBottom: 16,
  },
  subtitle: {
    fontSize: 18,
    color: '#8892b0',
  },
});
