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
        <div key={location.pathname} className="pmda-page-transition safe-bottom">
          <Outlet />
        </div>
      </SidebarInset>
    </SidebarProvider>
  );
}
