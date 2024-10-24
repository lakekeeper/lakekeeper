<template>
  <v-navigation-drawer v-model="visual.navBarShow">
    <v-list>
      <v-list-item
        link
        to="/"
        title="Home"
        prepend-icon="mdi-home"
      ></v-list-item>
      <v-list-item
        link
        to="catalogs"
        title="Catalogs"
        prepend-icon="mdi-book-open-variant"
      ></v-list-item>
      <v-list-item
        link
        to="settings"
        title="Settings"
        prepend-icon="mdi-cog"
      ></v-list-item>
    </v-list>
  </v-navigation-drawer>
</template>

<script setup lang="ts">
import { useVisualStore } from "../stores/visual";
const visual = useVisualStore();
</script>
