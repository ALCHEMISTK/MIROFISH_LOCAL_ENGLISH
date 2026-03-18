import service from './index'

export const getSetupStatus = () => service.get('/api/setup/status')

export const saveSetup = (data) => service.post('/api/setup/save', data)

export const validateSetup = (data) => service.post('/api/setup/validate', data)
