<template>
  <article class="article-card">
    <img v-if="article.image_url" :src="article.image_url" :alt="article.title" class="article-image" />
    <div class="article-content">
      <h2 class="article-title">{{ article.title }}</h2>
      <p class="article-summary">{{ article.summary }}</p>
      <div class="article-meta">
        <span class="source">{{ article.source_domain }}</span>
        <span class="date">{{ formatDate(article.published_at) }}</span>
      </div>
      <a :href="article.url" target="_blank" rel="noopener noreferrer" class="read-more">
        Read Full Article →
      </a>
    </div>
  </article>
</template>

<script setup lang="ts">
import type { Article } from '../api/articles';

defineProps<{
  article: Article;
}>();

const formatDate = (dateString: string) => {
  return new Date(dateString).toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric'
  });
};
</script>

<style scoped lang="scss">
.article-card {
  background: white;
  border: 1px solid var(--color-border);
  border-radius: var(--border-radius);
  overflow: hidden;
  transition: transform 0.2s, box-shadow 0.2s;

  &:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
  }
}

.article-image {
  width: 100%;
  height: 200px;
  object-fit: cover;
}

.article-content {
  padding: var(--spacing-md);
}

.article-title {
  font-size: 1.25rem;
  margin-bottom: var(--spacing-sm);
  color: var(--color-primary);
}

.article-summary {
  color: #666;
  margin-bottom: var(--spacing-sm);
  line-height: 1.5;
}

.article-meta {
  display: flex;
  justify-content: space-between;
  font-size: 0.875rem;
  color: #999;
  margin-bottom: var(--spacing-sm);
}

.read-more {
  color: var(--color-secondary);
  font-weight: 500;
  
  &:hover {
    text-decoration: underline;
  }
}
</style>
