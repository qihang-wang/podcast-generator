import { createRouter, createWebHistory } from 'vue-router';
import NewsList from '../views/NewsList.vue';
import ArticleDetail from '../views/ArticleDetail.vue';
import NotFound from '../views/NotFound.vue';

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      redirect: '/US'
    },
    {
      path: '/:country_code',
      name: 'news-list',
      component: NewsList
    },
    {
      path: '/article/:id',
      name: 'article-detail',
      component: ArticleDetail
    },
    {
      path: '/:pathMatch(.*)*',
      name: 'not-found',
      component: NotFound
    }
  ]
});

export default router;
