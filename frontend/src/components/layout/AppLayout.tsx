import { Outlet, useLocation } from 'react-router-dom';

import { Header } from '@/components/Header';
import { SidebarInset, SidebarProvider } from '@/components/ui/sidebar';
import { AppSidebar } from '@/components/layout/AppSidebar';
import { UiBuildWatcher } from '@/components/UiBuildWatcher';

export function AppLayout() {
  const location = useLocation();
  return (
    <SidebarProvider defaultOpen>
      <AppSidebar />
      <SidebarInset>
        <Header />
        <UiBuildWatcher />
        <div key={location.pathname} className="animate-in fade-in-0 duration-300">
          <Outlet />
        </div>
      </SidebarInset>
    </SidebarProvider>
  );
}
