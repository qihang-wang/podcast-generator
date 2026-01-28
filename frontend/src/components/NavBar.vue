<template>
  <nav class="navbar">
    <div class="container">
      <h1 class="logo">Global News</h1>
      <div class="country-selector">
        <select v-model="selectedCountry" @change="navigateToCountry">
          <option value="US">United States</option>
          <option value="UK">United Kingdom</option>
          <option value="CA">Canada</option>
          <option value="AU">Australia</option>
          <option value="FR">France</option>
          <option value="CH">China</option>
          <option value="JA">Japan</option>
          <option value="IN">India</option>
          <option value="BR">Brazil</option>
          <option value="RS">Russia</option>
          <option value="TH">Thailand</option>
        </select>
      </div>
    </div>
  </nav>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

const route = useRoute();
const router = useRouter();
const selectedCountry = ref(route.params.country_code as string || 'US');

watch(() => route.params.country_code, (newCode) => {
  if (newCode) selectedCountry.value = newCode as string;
});

const navigateToCountry = () => {
  router.push(`/${selectedCountry.value}`);
};
</script>

<style scoped lang="scss">
.navbar {
  background: var(--color-primary);
  color: white;
  padding: var(--spacing-sm) 0;
  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}

.container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 0 var(--spacing-md);
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.logo {
  font-size: 1.5rem;
  font-weight: bold;
}

select {
  padding: var(--spacing-xs) var(--spacing-sm);
  border-radius: var(--border-radius);
  border: none;
  font-size: 1rem;
  cursor: pointer;
}
</style>
