import { run } from '@proven-network/handler';
import { getCurrentIdentity } from '@proven-network/session';

/**
 * Get current user information
 */
export const getCurrentUser = run(() => {
  console.log('Getting current user identity');

  const identity = getCurrentIdentity();

  if (!identity) {
    console.log('No user currently authenticated');
    return { authenticated: false, user: null };
  }

  console.log('User is authenticated:', identity);
  return {
    authenticated: true,
    user: {
      id: identity || 'unknown',
      name: `User ${identity}`,
    },
  };
});

/**
 * Check if user is authenticated
 */
export const isAuthenticated = run((): boolean => {
  const identity = getCurrentIdentity();
  const isAuth = !!identity;

  console.log('Authentication check:', isAuth);
  return isAuth;
});

/**
 * Get user's todo permissions (example of authorization)
 */
export const getUserPermissions = run(() => {
  const identity = getCurrentIdentity();

  if (!identity) {
    console.log('No identity, returning guest permissions');
    return {
      canRead: false,
      canWrite: false,
      canDelete: false,
    };
  }

  // For this example, authenticated users have full permissions
  const permissions = {
    canRead: true,
    canWrite: true,
    canDelete: true,
  };

  console.log('User permissions:', permissions);
  return permissions;
});
