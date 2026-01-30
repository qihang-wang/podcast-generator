<template>
  <div class="article-detail">
    <div class="back-link">
      <router-link :to="`/${article?.country_code || 'US'}`">← 返回列表</router-link>
    </div>

    <div v-if="loading" class="loading">
      <div class="spinner"></div>
      <p>正在生成新闻内容，请稍候...</p>
    </div>

    <div v-else-if="error" class="error">
      <p>{{ error }}</p>
      <button @click="fetchDetail">重试</button>
    </div>

    <article v-else-if="article" class="content">
      <header>
        <h1>{{ article.title || '新闻详情' }}</h1>
        <div class="meta">
          <span class="source">{{ article.source }}</span>
          <span class="date">{{ formatDate(article.date_added) }}</span>
          <span v-if="cached" class="cached-badge">已缓存</span>
        </div>
      </header>

      <div class="llm-content" v-html="formattedContent"></div>

      <footer>
        <a :href="article.url" target="_blank" rel="noopener noreferrer" class="original-link">
          阅读原文 →
        </a>
      </footer>
    </article>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue';
import { useRoute } from 'vue-router';
import { articlesApi, type ArticleDetail } from '../api/articles';

const route = useRoute();
const loading = ref(true);
const error = ref('');
const article = ref<ArticleDetail['article'] | null>(null);
const llmContent = ref('');
const cached = ref(false);

const formattedContent = computed(() => {
  return llmContent.value.replace(/\n/g, '<br>');
});

const formatDate = (dateInt: number) => {
  const str = String(dateInt);
  if (str.length !== 14) return '';
  return `${str.slice(0, 4)}-${str.slice(4, 6)}-${str.slice(6, 8)}`;
};

const fetchDetail = async () => {
  loading.value = true;
  error.value = '';
  
  try {
    const articleId = route.params.id as string;
    const data = await articlesApi.getArticleDetail(articleId);
    article.value = data.article;
    llmContent.value = data.llm_content;
    cached.value = data.cached;
  } catch (e: any) {
    error.value = e.response?.data?.message || '获取文章详情失败';
  } finally {
    loading.value = false;
  }
};

onMounted(fetchDetail);
</script>

<style scoped lang="scss">
.article-detail {
  max-width: 800px;
  margin: 0 auto;
  padding: var(--spacing-lg);
}

.back-link {
  margin-bottom: var(--spacing-lg);
  
  a {
    color: var(--color-secondary);
    text-decoration: none;
    
    &:hover {
      text-decoration: underline;
    }
  }
}

.loading {
  text-align: center;
  padding: var(--spacing-xl) 0;
  
  .spinner {
    width: 40px;
    height: 40px;
    border: 3px solid var(--color-border);
    border-top-color: var(--color-primary);
    border-radius: 50%;
    animation: spin 1s linear infinite;
    margin: 0 auto var(--spacing-md);
  }
  
  p {
    color: #666;
  }
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.error {
  text-align: center;
  padding: var(--spacing-xl) 0;
  color: #e74c3c;
  
  button {
    margin-top: var(--spacing-md);
    padding: var(--spacing-sm) var(--spacing-lg);
    background: var(--color-primary);
    color: white;
    border: none;
    border-radius: var(--border-radius);
    cursor: pointer;
  }
}

.content {
  background: white;
  border-radius: var(--border-radius);
  padding: var(--spacing-lg);
  box-shadow: 0 2px 8px rgba(0,0,0,0.1);
  
  header {
    margin-bottom: var(--spacing-lg);
    padding-bottom: var(--spacing-md);
    border-bottom: 1px solid var(--color-border);
    
    h1 {
      font-size: 1.5rem;
      color: var(--color-primary);
      margin-bottom: var(--spacing-sm);
    }
    
    .meta {
      display: flex;
      gap: var(--spacing-md);
      font-size: 0.875rem;
      color: #666;
      
      .cached-badge {
        background: #27ae60;
        color: white;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 0.75rem;
      }
    }
  }
  
  .llm-content {
    line-height: 1.8;
    color: #333;
    font-size: 1rem;
  }
  
  footer {
    margin-top: var(--spacing-lg);
    padding-top: var(--spacing-md);
    border-top: 1px solid var(--color-border);
    
    .original-link {
      color: var(--color-secondary);
      font-weight: 500;
      
      &:hover {
        text-decoration: underline;
      }
    }
  }
}
</style>
