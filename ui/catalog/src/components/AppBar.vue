<template>
  <v-app-bar :elevation="2">
    <template v-slot:prepend>
      <v-app-bar-nav-icon :icon="navIcon" @click="navBar"></v-app-bar-nav-icon>
    </template>

    <v-app-bar-title>TIP Iceberg Catalog</v-app-bar-title>
    <v-spacer></v-spacer>

    <v-btn
      @click="toggleTheme"
      :icon="themeLight ? 'mdi-lightbulb-off' : 'mdi-lightbulb-on'"
      variant="text"
    ></v-btn>
  </v-app-bar>
</template>

<script setup lang="ts">
import { useTheme } from "vuetify";
import { useVisualStore } from "../stores/visual";
const visual = useVisualStore();

const theme = useTheme();
const themeLight = computed(() => {
  return visual.themeLight;
});

const themeText = computed(() => {
  return themeLight.value ? "light" : "dark";
});

const navIcon = computed(() => {
  return visual.navBarShow ? "mdi-menu-open" : "mdi-menu";
});

ref("mdi-menu");

onMounted(() => {
  theme.global.name.value = themeText.value;
});
function toggleTheme() {
  visual.toggleThemeLight();
  theme.global.name.value = themeText.value;
}

function navBar() {
  visual.navBarSwitch();
}
</script>
