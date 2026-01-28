import { createRouter, createWebHistory } from 'vue-router';
import NewsList from '../views/NewsList.vue';
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
      path: '/:pathMatch(.*)*',
      name: 'not-found',
      component: NotFound
    }
  ]
});

export default router;
