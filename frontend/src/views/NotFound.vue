<template>
  <div class="not-found">
    <h1>404</h1>
    <p>Page not found</p>
    <router-link to="/US">Go to Home</router-link>
  </div>
</template>

<style scoped lang="scss">
.not-found {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  min-height: calc(100vh - 80px);
  text-align: center;

  h1 {
    font-size: 4rem;
    color: var(--color-primary);
  }

  p {
    font-size: 1.5rem;
    margin: var(--spacing-md) 0;
  }

  a {
    color: var(--color-secondary);
    font-weight: 500;
    
    &:hover {
      text-decoration: underline;
    }
  }
}
</style>
