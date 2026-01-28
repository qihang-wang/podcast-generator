<template>
  <div class="news-list">
    <div class="container">
      <h1 class="page-title">{{ countryName }} News</h1>
      
      <div v-if="loading" class="loading">Loading articles...</div>
      <div v-else-if="error" class="error">{{ error }}</div>
      <div v-else-if="articles.length === 0" class="empty">No articles found</div>
      
      <div v-else class="articles-grid">
        <ArticleCard v-for="article in articles" :key="article.id" :article="article" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from 'vue';
import { useRoute } from 'vue-router';
import { useHead } from '@unhead/vue';
import ArticleCard from '../components/ArticleCard.vue';
import { articlesApi, type Article } from '../api/articles';

const route = useRoute();
const articles = ref<Article[]>([]);
const loading = ref(true);
const error = ref('');

const countryNames: Record<string, string> = {
  US: 'United States', UK: 'United Kingdom', CA: 'Canada',
  AU: 'Australia', FR: 'France', CH: 'China', JA: 'Japan',
  IN: 'India', BR: 'Brazil', RS: 'Russia', TH: 'Thailand'
};

const countryName = ref(countryNames[route.params.country_code as string] || 'Global');

useHead({
  title: () => `${countryName.value} News - Global News`,
  meta: [
    { name: 'description', content: () => `Latest news from ${countryName.value}` }
  ]
});

const fetchArticles = async () => {
  loading.value = true;
  error.value = '';
  try {
    const countryCode = route.params.country_code as string;
    countryName.value = countryNames[countryCode] || 'Global';
    articles.value = await articlesApi.getArticles(countryCode);
  } catch (e) {
    error.value = 'Failed to load articles. Please try again later.';
    console.error(e);
  } finally {
    loading.value = false;
  }
};

onMounted(fetchArticles);
watch(() => route.params.country_code, fetchArticles);
</script>

<style scoped lang="scss">
.news-list {
  min-height: calc(100vh - 80px);
  padding: var(--spacing-lg) 0;
}

.container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 0 var(--spacing-md);
}

.page-title {
  font-size: 2rem;
  margin-bottom: var(--spacing-lg);
  color: var(--color-primary);
}

.articles-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
  gap: var(--spacing-lg);
}

.loading, .error, .empty {
  text-align: center;
  padding: var(--spacing-lg);
  font-size: 1.125rem;
}

.error {
  color: #dc2626;
}
</style>
