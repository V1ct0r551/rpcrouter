import { useQuery } from '@tanstack/react-query';
import { apiFetch } from './client'; import type { ChainList, ChainRow, Overview, StateInfo } from './types';
export const useOverview = (interval: number) => useQuery({ queryKey: ['overview'], queryFn: () => apiFetch<Overview>('/admin/api/overview'), refetchInterval: interval });
export const useChains = (params: URLSearchParams, interval: number) => useQuery({ queryKey: ['chains', params.toString()], queryFn: () => apiFetch<ChainList>(`/admin/api/chains?${params}`), refetchInterval: interval });
export const useChain = (id: string | undefined, interval: number) => useQuery({ queryKey: ['chain', id], queryFn: () => apiFetch<ChainRow>(`/admin/api/chains/${id}`), enabled: Boolean(id), refetchInterval: interval });
export const useStateInfo = (interval: number) => useQuery({ queryKey: ['state'], queryFn: () => apiFetch<StateInfo>('/admin/api/state'), refetchInterval: interval });
