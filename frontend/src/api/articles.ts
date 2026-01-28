import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

export interface Article {
  id: string;
  title: string;
  summary: string;
  url: string;
  source_domain: string;
  published_at: string;
  country_code: string;
  tone?: number;
  image_url?: string;
}

export const articlesApi = {
  async getArticles(countryCode: string): Promise<Article[]> {
    const response = await axios.get(`${API_BASE_URL}/api/articles/`, {
      params: { country_code: countryCode, days: 0 }
    });
    return response.data.data?.articles || [];
  }
};
