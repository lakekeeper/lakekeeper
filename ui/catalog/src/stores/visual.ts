// Utilities
import { defineStore } from "pinia";

export const useVisualStore = defineStore(
  "visual",
  () => {
    const themeLight = ref(true);
    const navBarShow = ref(true);

    function toggleThemeLight() {
      themeLight.value = !themeLight.value;
    }

    function navBarSwitch() {
      navBarShow.value = !navBarShow.value;
    }

    return { themeLight, navBarShow, toggleThemeLight, navBarSwitch };
  },
  {
    persistedState: {
      key: "visual",
      persist: true,
    },
  }
);
