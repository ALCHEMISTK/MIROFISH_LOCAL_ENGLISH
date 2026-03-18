import { createRouter, createWebHistory } from 'vue-router'
import Home from '../views/Home.vue'
import Process from '../views/MainView.vue'
import SimulationView from '../views/SimulationView.vue'
import SimulationRunView from '../views/SimulationRunView.vue'
import ReportView from '../views/ReportView.vue'
import InteractionView from '../views/InteractionView.vue'
import SetupView from '../views/SetupView.vue'
import { getSetupState, setSetupStatus } from '../store/setupStatus'
import { getSetupStatus } from '../api/setup'

const routes = [
  {
    path: '/setup',
    name: 'Setup',
    component: SetupView
  },
  {
    path: '/',
    name: 'Home',
    component: Home
  },
  {
    path: '/process/:projectId',
    name: 'Process',
    component: Process,
    props: true
  },
  {
    path: '/simulation/:simulationId',
    name: 'Simulation',
    component: SimulationView,
    props: true
  },
  {
    path: '/simulation/:simulationId/start',
    name: 'SimulationRun',
    component: SimulationRunView,
    props: true
  },
  {
    path: '/report/:reportId',
    name: 'Report',
    component: ReportView,
    props: true
  },
  {
    path: '/interaction/:reportId',
    name: 'Interaction',
    component: InteractionView,
    props: true
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

router.beforeEach(async (to, from, next) => {
  if (to.name === 'Setup') return next()

  const state = getSetupState()

  // Use cached status if already checked
  if (state.checked) {
    if (!state.configured) return next({ name: 'Setup' })
    return next()
  }

  // Check with backend
  try {
    const res = await getSetupStatus()
    const configured = res.configured
    setSetupStatus(configured)
    if (!configured) return next({ name: 'Setup' })
    next()
  } catch (e) {
    // Backend not reachable - let user proceed (they'll see errors)
    setSetupStatus(true)
    next()
  }
})

export default router
