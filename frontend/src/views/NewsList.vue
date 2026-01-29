<template>
  <div class="news-list">
    <div class="container">
      <h1 class="page-title">{{ countryName }} News</h1>
      
      <div v-if="loading" class="loading">Loading articles...</div>
      <div v-else-if="error" class="error">{{ error }}</div>
      <div v-else-if="articles.length === 0" class="empty">No articles found</div>
      
      <template v-else>
        <div class="articles-grid">
          <ArticleCard v-for="article in articles" :key="article.id" :article="article" />
        </div>
        
        <div v-if="totalPages > 1" class="pagination">
          <button @click="goToPage(currentPage - 1)" :disabled="currentPage <= 1">← Prev</button>
          <span class="page-info">Page {{ currentPage }} of {{ totalPages }} ({{ total }} articles)</span>
          <button @click="goToPage(currentPage + 1)" :disabled="currentPage >= totalPages">Next →</button>
        </div>
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from 'vue';
import { useRoute } from 'vue-router';
import ArticleCard from '../components/ArticleCard.vue';
import { articlesApi, type Article } from '../api/articles';

const route = useRoute();
const articles = ref<Article[]>([]);
const loading = ref(true);
const error = ref('');
const currentPage = ref(1);
const total = ref(0);
const totalPages = ref(0);

const countryNames: Record<string, string> = {
  US: 'United States', UK: 'United Kingdom', CA: 'Canada',
  AU: 'Australia', FR: 'France', CH: 'China', JA: 'Japan',
  IN: 'India', BR: 'Brazil', RS: 'Russia', TH: 'Thailand'
};

const countryName = ref(countryNames[route.params.country_code as string] || 'Global');

const fetchArticles = async (page: number = 1) => {
  loading.value = true;
  error.value = '';
  try {
    const countryCode = route.params.country_code as string;
    countryName.value = countryNames[countryCode] || 'Global';
    document.title = `${countryName.value} News - Global News`;
    const result = await articlesApi.getArticles(countryCode, page);
    articles.value = result.articles;
    total.value = result.total;
    totalPages.value = result.totalPages;
    currentPage.value = page;
  } catch (e) {
    error.value = 'Failed to load articles. Please try again later.';
    console.error(e);
  } finally {
    loading.value = false;
  }
};

const goToPage = (page: number) => {
  if (page >= 1 && page <= totalPages.value) {
    fetchArticles(page);
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }
};

onMounted(() => fetchArticles());
watch(() => route.params.country_code, () => fetchArticles(1));
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

.pagination {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: var(--spacing-md);
  margin-top: var(--spacing-lg);
  padding: var(--spacing-md) 0;
  
  button {
    padding: var(--spacing-xs) var(--spacing-md);
    border: 1px solid var(--color-border);
    border-radius: var(--border-radius);
    background: white;
    cursor: pointer;
    transition: all 0.2s;
    
    &:hover:not(:disabled) {
      background: var(--color-primary);
      color: white;
    }
    
    &:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }
  }
  
  .page-info {
    color: #666;
  }
}
</style>
